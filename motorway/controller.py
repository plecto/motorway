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

HEARTBEAT_TIMEOUT = 30


def resolve_tree(stream_consumers, producer):
    streams = {}
    for stream, dct in stream_consumers.items():
        if producer in dct['producers']:
            intersections = {}
            for consumer in dct['consumers']:
                intersections[consumer] = resolve_tree(stream_consumers, consumer)
            streams[stream] = intersections
    return streams


class ControllerIntersection(Intersection):

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
            'histogram': {minute: {'error_count': 0, 'success_count': 0, 'timeout_count': 0, 'processed_count': 0}.copy() for minute in range(0, 60)}.copy()
        }.copy()

    def __init__(self, stream_consumers, context, controller_bind_address, web_server=True):
        super(ControllerIntersection, self).__init__()
        self.stream_consumers = stream_consumers
        self.ramp_socks = {}
        self.messages = {}
        self.failed_messages = {}
        self.process_statistics = {}
        self.waiting_messages = {}
        self.queue_processes = {}
        self.context = context
        self.controller_bind_address = controller_bind_address
        self.process_id_to_name = {}  # Maps UUIDs to human readable names
        self.process_address_to_uuid = {}  # Maps tcp endpoints to human readable names

    @batch_process(wait=1, limit=500)
    def process(self, messages):
        for message in messages:
            original_process = message.producer_uuid
            if original_process not in self.process_statistics:
                self.process_statistics[original_process] = self.get_default_process_dict()

            current_process = message.content['process_name']
            if current_process not in self.process_statistics:
                self.process_statistics[current_process] = self.get_default_process_dict()

            destination_process = message.destination_uuid or self.process_address_to_uuid[message.destination_endpoint]
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
                self.fail(message.ramp_unique_id, error_message=message.error_message, process=original_process)
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
        # Check message status
        waiting_messages = {}
        for unique_id, lst in self.messages.items():
            original_process, ack_value, start_time, process = lst
            if unique_id in self.failed_messages:
                del self.messages[unique_id]  # This failed somewhere else in the chain and it was notificed already
            elif (now - start_time) > datetime.timedelta(minutes=30):
                del self.messages[unique_id]  # clean up
                self.process_statistics[process]['histogram'][datetime.datetime.now().minute]['timeout_count'] += 1
                self.fail(unique_id, original_process, error_message="Message timed out")
            elif ack_value > 0:
                waiting_messages[process] = waiting_messages.get(process, 0) + 1
        self.waiting_messages = waiting_messages

        # Update histograms
        for process in self.process_statistics.keys():
            self.process_statistics[process]['histogram'][(now + datetime.timedelta(minutes=1)).minute] = self.get_default_process_dict()['histogram'][0]  # reset next minute
            self.process_statistics[process]['waiting'] = self.waiting_messages.get(process, 0)

        message = Message("_controller-%s" % uuid.uuid4(), {
            'process_id_to_name': self.process_id_to_name,
            'process_statistics': self.process_statistics,
            'stream_consumers': self.stream_consumers,
            'failed_messages': self.failed_messages,
        })

        if self.send_socks:  # Send updates to webserves. Uses the same code as intersection._process - consider deduplicating it
            socket_address = self.get_grouper(self.send_grouper)(
                self.send_socks.keys()
            ).get_destination_for(message.grouping_value)
            message.send(
                self.send_socks[socket_address],
                self.process_uuid
            )

    def _update_wrapper(self):
        while True:
            self.update()
            time.sleep(1)

    def fail(self, unique_id, process, error_message=""):
        self.failed_messages[unique_id] = (process, error_message)
        if process not in self.ramp_socks:
            logger.debug("%s not in ramp_socks, probably a intersection which doesn't support feedback. Had %s" % (process, self.ramp_socks))
        else:
            self.ramp_socks[process].send_json({
                'status': 'fail',
                'id': unique_id
            })

    def success(self, unique_id, process):
        if process not in self.ramp_socks:
            logger.debug("%s not in ramp_socks, probably a intersection which doesn't support feedback. Had %s" % (process, self.ramp_socks))
        else:
            self.ramp_socks[process].send_json({
                'status': 'success',
                'id': unique_id
            })

    def update_connections(self, output_queue):
        update_connection_sock = self.context.socket(zmq.PULL)  # The socket for receiving updates from clients
        update_connection_port = update_connection_sock.bind_to_random_port("tcp://*")
        set_timeouts_on_socket(update_connection_sock)

        refresh_connection_sock = self.context.socket(zmq.PUB)  # The broadcast socket where clients subscribe to updates
        refresh_connection_sock.bind(self.controller_bind_address)
        set_timeouts_on_socket(refresh_connection_sock)

        self.queue_processes['_update_connections'] = {
            'streams': ['tcp://127.0.0.1:%d' % update_connection_port],
            'grouping': None,
            'stream_heartbeats': {}
        }
        refresh_connection_sock.send_json(self.queue_processes)  # Initial refresh/broadcast

        poller = zmq.Poller()
        poller.register(update_connection_sock, zmq.POLLIN)

        current_heartbeat = lambda: calendar.timegm(datetime.datetime.utcnow().timetuple())

        while True:
            socks = dict(poller.poll(timeout=10000))
            if socks.get(update_connection_sock) == zmq.POLLIN:
                connection_updates = update_connection_sock.recv_json()
                self.process_id_to_name[connection_updates['meta']['id']] = connection_updates['meta']['name']
                for queue, consumers in connection_updates['streams'].items():
                    if queue not in self.queue_processes:
                        self.queue_processes[queue] = {
                            'streams': [],
                            'stream_heartbeats': {},
                            'grouping': None
                        }
                    for consumer in consumers:
                        if consumer not in self.queue_processes[queue]['streams']:
                            self.process_address_to_uuid[consumer] = connection_updates['meta']['id']
                            # Add to streams and update grouping TODO: Keep track of multiple groupings
                            self.queue_processes[queue]['streams'].append(consumer)
                            self.queue_processes[queue]['grouping'] = connection_updates['meta']['grouping']
                            if '_ramp' in queue:  # Ramp replies
                                self.ramp_socks[queue] = self.context.socket(zmq.PUSH)
                                self.ramp_socks[queue].connect(consumer)
                        self.queue_processes[queue]['stream_heartbeats'][consumer] = {
                            'heartbeat': current_heartbeat(),
                            'process_name': connection_updates['meta']['name'],
                            'process_id': connection_updates['meta']['id']
                        }
                        logging.debug("Received heartbeat from %s on queue %s - current value %s" % (consumer, queue, self.queue_processes[queue]['stream_heartbeats'][consumer]))
            for queue, consumers in self.queue_processes.items():
                for consumer, heartbeat_info in consumers['stream_heartbeats'].items():
                    if current_heartbeat() > (heartbeat_info['heartbeat'] + HEARTBEAT_TIMEOUT):
                        logger.warn("Removing %s from %s due to missing heartbeat" % (consumer, queue))
                        self.queue_processes[queue]['streams'].remove(consumer)
                        self.queue_processes[queue]['stream_heartbeats'].pop(consumer)
                        self.process_statistics[heartbeat_info['process_id']]['status'] = 'failed'

            refresh_connection_sock.send_json(self.queue_processes)
            logger.debug("Announced %s", self.queue_processes)
            # Update our own send socks
            if output_queue and output_queue in self.queue_processes:
                self.set_send_socks(self.queue_processes, output_queue, self.context)

    def _process_wrapper(self):
        message_ack_sock = self.context.socket(zmq.PULL)
        message_ack_port = message_ack_sock.bind_to_random_port("tcp://*")
        self.queue_processes['_message_ack'] = {
            'streams': ['tcp://127.0.0.1:%s' % message_ack_port],
            'grouping': None,
            'stream_heartbeats': {}
        }
        set_timeouts_on_socket(message_ack_sock)
        while True:
            self._process(message_ack_sock, None)


    @classmethod
    def run(cls, input_stream, output_stream=None, refresh_connection_stream=None, grouper_cls=None):

        setproctitle("data-pipeline: %s" % cls.__name__)
        logger.info("Running %s" % cls.__name__)

        context = zmq.Context()

        self = cls(
            {},
            context,
            refresh_connection_stream
        )

        # Create Thread Factories :-)

        thread_update_connections_factory = lambda: Thread(target=self.update_connections, name="controller-update_connections", args=(output_stream, ))
        thread_update_stats_factory = lambda: Thread(target=self._update_wrapper, name="controller-update_stats")
        thread_process_factory = lambda: Thread(target=self._process_wrapper, name="controller-process_acks")

        # Run threads

        thread_update_connections = thread_update_connections_factory()
        thread_update_connections.start()

        thread_update_stats = thread_update_stats_factory()
        thread_update_stats.start()

        thread_process = thread_process_factory()
        thread_process.start()

        while True:
            if not thread_update_connections.isAlive():
                logger.error("Thread thread_update_connections crashed in %s" % self.__class__.__name__)
                thread_update_connections = thread_update_connections_factory()
                thread_update_connections.start()
            if not thread_update_stats.isAlive():
                logger.error("Thread thread_update_stats crashed in %s" % self.__class__.__name__)
                thread_update_stats = thread_update_stats_factory()
                thread_update_stats.start()
            if not thread_process.isAlive():
                logger.error("Thread thread_process crashed in %s" % self.__class__.__name__)
                thread_process = thread_process_factory()
                thread_process.start()
            time.sleep(10)
