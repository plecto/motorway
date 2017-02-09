import calendar
import logging
from setproctitle import setproctitle
from threading import Thread
import time
import datetime
import uuid
from motorway.decorators import batch_process
from motorway.messages import Message
from motorway.intersection import Intersection
from motorway.utils import percentile_from_dict, set_timeouts_on_socket
from isodate import parse_duration
import zmq


logger = logging.getLogger(__name__)


class ControllerIntersection(Intersection):
    """
    Responsible for keeping track of the state of each message.

    We're using an constant storage algorithm to track indefinite state. The cost of the constant storage is that
    we never know how far it is in the "tree" of intersections it has to pass through. We create a bunch of dictionaries
    to count how many pending messages each intersection has to provide useful stats in the web UI.
    """

    send_control_messages = False
    MESSAGE_TIMEOUT = 30  # minutes

    def __init__(self, stream_consumers=None, **kwargs):
        super(ControllerIntersection, self).__init__(**kwargs)
        self.stream_consumers = stream_consumers or {}
        self.ramp_socks = {}
        self.messages = {}
        self.failed_messages = {}
        self.process_statistics = {}
        self.queue_processes = {}

    def get_default_process_dict(self):
        return {
            'status': 'running',
            'success': 0,
            'failed': 0,
            'processed': 0,
            'waiting': 0,
            'time_taken': datetime.timedelta(seconds=0),
            'avg_time_taken': datetime.timedelta(seconds=0),
            '95_percentile': datetime.timedelta(seconds=0),
            'frequency': {}.copy(),
            'total_frequency': 0,
            'histogram': {
                minute: {
                    'error_count': 0, 'success_count': 0, 'timeout_count': 0, 'processed_count': 0
                }.copy() for minute in range(0, 60)
            }.copy()
        }.copy()


    @batch_process(wait=1, limit=500)
    def process(self, messages):
        for message in messages:
            original_process = message.producer_uuid
            if original_process not in self.process_statistics:
                self.process_statistics[original_process] = self.get_default_process_dict()

            current_process = message.content['process_name']
            if current_process not in self.process_statistics:
                self.process_statistics[current_process] = self.get_default_process_dict()

            destination_process = message.destination_uuid or self.process_address_to_uuid[message.destination_endpoint]  # Usually we should only have the endpoint in the message
            if destination_process not in self.process_statistics:
                self.process_statistics[destination_process] = self.get_default_process_dict()

            # Create message or update with ack value
            if message.ramp_unique_id not in self.messages:  # Message just created
                assert message.producer_uuid, "Producer UUID missing from %s" % message._message()
                self.messages[message.ramp_unique_id] = [
                    message.producer_uuid,
                    message.ack_value,
                    datetime.datetime.now(),
                    destination_process
                ]  # Set the new value to ack value
            elif message.ack_value >= 0:  # Message processed
                self.messages[message.ramp_unique_id][1] ^= message.ack_value  # XOR the existing value
                # Update process information
                if self.messages[message.ramp_unique_id][1] == Message.SUCCESS:
                    original_process = self.messages[message.ramp_unique_id][0]
                    self.process_statistics[original_process]['success'] += 1
                    self.process_statistics[original_process]['histogram'][datetime.datetime.now().minute]['success_count'] += 1
                    self.success(message.ramp_unique_id, original_process)
                    del self.messages[message.ramp_unique_id]
                else:  # Still not finished - update destination!
                    if destination_process != original_process:
                        self.messages[message.ramp_unique_id][3] = destination_process  # Update destination so we can show what is waiting for this process!
            elif message.ack_value == Message.FAIL:
                if message.ramp_unique_id in self.messages:
                    original_process, ack_value, start_time, destination_process = self.messages[message.ramp_unique_id]
                    del self.messages[message.ramp_unique_id]
                self.process_statistics[original_process]['failed'] += 1
                self.process_statistics[original_process]['histogram'][datetime.datetime.now().minute]['error_count'] += 1
                self.fail(
                    message.ramp_unique_id,
                    error_message=message.error_message,
                    process=original_process,
                    message_content=message.content['message_content']
                )
            self.process_statistics[original_process]['processed'] += 1

            # Update statistics
            if 'duration' in message.content:
                time_taken = parse_duration(message.content['duration'])
                rounded_seconds = round(time_taken.total_seconds(), 0)
                self.process_statistics[current_process]['time_taken'] += time_taken
                if ('msg_type' in message.content and message.content['msg_type'] != "new_msg") or ('sender' in message.content and message.content['sender'] == 'ramp'):
                    self.process_statistics[current_process]['histogram'][datetime.datetime.now().minute]['processed_count'] += 1
                    self.process_statistics[current_process]['frequency'][rounded_seconds] = self.process_statistics[current_process]['frequency'].get(rounded_seconds, 0) + 1
                    self.process_statistics[current_process]['total_frequency'] = sum(self.process_statistics[current_process]['frequency'].values())
                    self.process_statistics[current_process]['95_percentile'] = datetime.timedelta(seconds=percentile_from_dict(self.process_statistics[current_process]['frequency'], 95))
                    self.process_statistics[current_process]['avg_time_taken'] = self.process_statistics[current_process]['time_taken'] / sum(self.process_statistics[current_process]['frequency'].values())
        yield  # Hack: This is actually done by self.update() to trigger it even if there are no messages and to reduce messages to 1/s

    def update(self):
        now = datetime.datetime.now()

        process_uuid_to_address = {uuid: address for address, uuid in self.process_address_to_uuid.items()}

        # Check message status
        waiting_messages = {}
        for unique_id, lst in self.messages.items():
            original_process, ack_value, start_time, process = lst
            if unique_id in self.failed_messages:
                del self.messages[unique_id]  # This failed somewhere else in the chain and it was notificed already
            elif process_uuid_to_address and process not in process_uuid_to_address:  # check if dict is empty before checking if the process is in it (mainly for tests)
                del self.messages[unique_id]  # clean up
                self.process_statistics[process]['histogram'][datetime.datetime.now().minute]['timeout_count'] += 1
                self.fail(unique_id, original_process, error_message="Assigned processed disappeared")
            elif (now - start_time) > datetime.timedelta(minutes=self.MESSAGE_TIMEOUT):
                del self.messages[unique_id]  # clean up
                self.process_statistics[process]['histogram'][datetime.datetime.now().minute]['timeout_count'] += 1
                self.fail(unique_id, original_process, error_message="Message timed out")
            elif ack_value > 0:
                waiting_messages[process] = waiting_messages.get(process, 0) + 1

        # Update histograms
        for process in self.process_statistics.keys():
            self.process_statistics[process]['histogram'][(now + datetime.timedelta(minutes=1)).minute] = self.get_default_process_dict()['histogram'][0]  # reset next minute
            self.process_statistics[process]['waiting'] = waiting_messages.get(process, 0)

        # Prepare message but copy the dictionaries so we avoid them being changed while we send it over ZMQ
        message = Message("_controller-%s" % uuid.uuid4(), {
            'process_id_to_name': self.process_id_to_name.copy(),
            'process_statistics': self.process_statistics.copy(),
            'stream_consumers': self.stream_consumers.copy(),
            'failed_messages': self.failed_messages.copy(),
        }, grouping_value=str(self.process_uuid))

        if self.send_socks:  # if we have at least one destination, send the updates
            self.send_message(message, self.process_uuid, control_message=False)

    def set_send_socks(self, connections, output_queue, context):
        super(ControllerIntersection, self).set_send_socks(connections, output_queue, context)

        # Override to look for ramps as well, which only is relevant for controller at the moment
        for queue, queue_info in connections.items():
            if '_ramp' in queue:  # Ramp replies
                if queue not in self.ramp_socks or not self.ramp_socks[queue]:
                    self.ramp_socks[queue] = context.socket(zmq.PUSH)
                    self.ramp_socks[queue].connect(queue_info['streams'][0])  # There should always be exactly one stream for a ramp

    def _update_wrapper(self):
        while True:
            self.update()
            time.sleep(1)

    def fail(self, unique_id, process, error_message="", message_content=None):
        # Use tuple because we will be transmitting them over JSON on large quantities and dict is too verbose
        self.failed_messages[unique_id] = (datetime.datetime.now(), process, error_message, message_content)
        if process not in self.ramp_socks and '_ramp' in process:
            logger.warn("%s not in ramp_socks. Had %s" % (process, self.ramp_socks))
        elif process in self.ramp_socks:
            self.ramp_socks[process].send_json({
                'status': 'fail',
                'id': unique_id
            })

    def success(self, unique_id, process):
        if process not in self.ramp_socks and '_ramp' in process:
            logger.warn("%s not in ramp_socks. Had %s" % (process, self.ramp_socks))
        elif process in self.ramp_socks:
            self.ramp_socks[process].send_json({
                'status': 'success',
                'id': unique_id
            })

    def thread_factory(self, *args, **kwargs):
        factories = super(ControllerIntersection, self).thread_factory(*args, **kwargs)
        thread_update_stats_factory = lambda: Thread(target=self._update_wrapper, name="controller-update_stats")

        return factories + [thread_update_stats_factory]