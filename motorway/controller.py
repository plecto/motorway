from collections import Counter
import json
import logging
from setproctitle import setproctitle
from threading import Thread
import time
import datetime
from motorway.decorators import batch_process
from motorway.messages import Message
from motorway.intersection import Intersection
from motorway.utils import ramp_result_stream_name, percentile_from_dict, DateTimeAwareJsonEncoder, \
    set_timeouts_on_socket
from flask import Flask, render_template, Response
from isodate import parse_duration
import zmq


logger = logging.getLogger(__name__)


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
            'success': 0,
            'failed': 0,
            'processed': 0,
            'waiting': 0,
            'time_taken': datetime.timedelta(seconds=0),
            'avg_time_taken': datetime.timedelta(seconds=0),
            '95_percentile': datetime.timedelta(seconds=0),
            'frequency': {}.copy(),
            'histogram': {minute: {'error_count': 0, 'success_count': 0}.copy() for minute in range(0, 60)}.copy()
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

        # for stream_dict in stream_consumers.values():
        #     for process in stream_dict['producers'] + stream_dict['consumers']:
        #         if not process in self.process_statistics:
        #             self.process_statistics[process] = self.get_default_process_dict()


        if web_server:
            app = Flask(__name__)
            @app.route("/")
            def hello():
                return render_template("index.html")

            @app.route("/json/")
            def json_output():
                now = datetime.datetime.now()
                return Response(json.dumps(dict(
                    sorted_process_statistics=sorted(
                        [(self.process_id_to_name.get(process_id, process_id), stats) for process_id, stats in self.process_statistics.items()],
                        key=lambda itm: itm[0]
                    ),
                    stream_consumers=self.stream_consumers,
                    # messages=self.messages,
                    failed_messages=self.failed_messages,
                    last_minutes=[(now - datetime.timedelta(minutes=i)).minute for i in range(0, 10)]
                ), cls=DateTimeAwareJsonEncoder), mimetype='application/json')

            p = Thread(target=app.run, name="controller-web", kwargs=dict(
                port=5000,
                host="0.0.0.0",
            ))
            p.start()

    @batch_process(wait=1, limit=500)
    def process(self, messages):
        for message in messages:
            process = message.content['process_name']
            if process not in self.process_statistics:
                self.process_statistics[process] = self.get_default_process_dict()

            # Create message or update with ack value
            if message.ramp_unique_id not in self.messages:  # Message just created
                self.messages[message.ramp_unique_id] = [process, message.ack_value, datetime.datetime.now()]  # Set the new value to ack value
            elif message.ack_value >= 0:  # Message processed
                self.messages[message.ramp_unique_id][1] ^= message.ack_value  # XOR the existing value
                # Update process information
                if self.messages[message.ramp_unique_id][1] == Message.SUCCESS:
                    original_process = self.messages[message.ramp_unique_id][0]
                    self.process_statistics[original_process]['success'] += 1
                    self.process_statistics[original_process]['histogram'][datetime.datetime.now().minute]['success_count'] += 1
                    self.success(message.ramp_unique_id, original_process)
                    del self.messages[message.ramp_unique_id]
            elif message.ack_value == Message.FAIL:
                if message.ramp_unique_id in self.messages:
                    process, ack_value, start_time = self.messages[message.ramp_unique_id]
                del self.messages[message.ramp_unique_id]
                self.process_statistics[process]['failed'] += 1
                self.process_statistics[process]['histogram'][datetime.datetime.now().minute]['error_count'] += 1
                self.fail(message.ramp_unique_id, error_message=message.error_message, process=process)
            self.process_statistics[process]['processed'] += 1

            # Update statistics
            if 'duration' in message.content:
                time_taken = parse_duration(message.content['duration'])
                rounded_seconds = round(time_taken.total_seconds(), 0)
                self.process_statistics[process]['time_taken'] += time_taken
                self.process_statistics[process]['frequency'][rounded_seconds] = self.process_statistics[process]['frequency'].get(rounded_seconds, 0) + 1
                self.process_statistics[process]['95_percentile'] = datetime.timedelta(seconds=percentile_from_dict(self.process_statistics[process]['frequency'], 95))
                self.process_statistics[process]['avg_time_taken'] = self.process_statistics[process]['time_taken'] / sum(self.process_statistics[process]['frequency'].values())
        yield

    def update(self):
        now = datetime.datetime.now()
        # Check message status
        waiting_messages = {}
        for unique_id, lst in self.messages.items():
            process, ack_value, start_time = lst
            if unique_id in self.failed_messages:
                del self.messages[unique_id]  # This failed somewhere else in the chain and it was notificed already
            elif (now - start_time) > datetime.timedelta(minutes=30):
                del self.messages[unique_id] # clean up
                self.fail(unique_id, process, error_message="Message timed out")
            elif ack_value > 0:
                waiting_messages[process] = waiting_messages.get(process, 0) + 1
        self.waiting_messages = waiting_messages

        # Update histograms
        for process in self.process_statistics.keys():
            self.process_statistics[process]['histogram'][(now + datetime.timedelta(minutes=1)).minute] = self.get_default_process_dict()['histogram'][0]  # reset next minute
            self.process_statistics[process]['waiting'] = self.waiting_messages.get(process, 0)

    def fail(self, unique_id, process, error_message=""):
        self.failed_messages[unique_id] = (process, error_message)
        if process not in self.ramp_socks:
            logging.debug("%s not in ramp_socks, probably a intersection which doesn't support feedback. Had %s" % (process, self.ramp_socks))
        else:
            self.ramp_socks[process].send_json({
                'status': 'fail',
                'id': unique_id
            })

    def success(self, unique_id, process):
        if process not in self.ramp_socks:
            logging.debug("%s not in ramp_socks, probably a intersection which doesn't support feedback. Had %s" % (process, self.ramp_socks))
        else:
            self.ramp_socks[process].send_json({
                'status': 'success',
                'id': unique_id
            })

    def update_connections(self):
        update_connection_sock = self.context.socket(zmq.PULL)
        update_connection_port = update_connection_sock.bind_to_random_port("tcp://*")
        set_timeouts_on_socket(update_connection_sock)

        refresh_connection_sock = self.context.socket(zmq.PUB)
        refresh_connection_sock.bind(self.controller_bind_address)
        set_timeouts_on_socket(refresh_connection_sock)

        self.queue_processes['_update_connections'] = ['tcp://127.0.0.1:%d' % update_connection_port]
        refresh_connection_sock.send_json(self.queue_processes)  # Initial refresh

        poller = zmq.Poller()
        poller.register(update_connection_sock, zmq.POLLIN)

        while True:
            socks = dict(poller.poll(timeout=10000))
            if socks.get(update_connection_sock) == zmq.POLLIN:
                connection_updates = update_connection_sock.recv_json()
                self.process_id_to_name[connection_updates['meta']['id']] = connection_updates['meta']['name']
                for queue, consumers in connection_updates['streams'].items():
                    if queue not in self.queue_processes:
                        self.queue_processes[queue] = []
                    for consumer in consumers:
                        if consumer not in self.queue_processes[queue]:
                            self.queue_processes[queue].append(consumer)
                            if '_ramp' in queue:
                                self.ramp_socks[queue] = self.context.socket(zmq.PUSH)
                                self.ramp_socks[queue].connect(consumer)
            refresh_connection_sock.send_json(self.queue_processes)
            logger.debug("Announced %s", self.queue_processes)

    def _process_wrapper(self):
        message_ack_sock = self.context.socket(zmq.PULL)
        message_ack_port = message_ack_sock.bind_to_random_port("tcp://*")
        self.queue_processes['_message_ack'] = ['tcp://127.0.0.1:%s' % message_ack_port]
        set_timeouts_on_socket(message_ack_sock)
        while True:
            self._process(message_ack_sock, None, None)


    @classmethod
    def run(cls, controller_bind_address, stream_consumers):

        setproctitle("data-pipeline: %s" % cls.__name__)
        logger.info("Running %s" % cls.__name__)

        context = zmq.Context()

        self = cls(
            stream_consumers,
            context,
            controller_bind_address
        )

        thread_update_connections = Thread(target=self.update_connections, name="controller-update_connections")
        thread_update_connections.start()

        thread_update_stats = Thread(target=self.update, name="controller-update_stats")
        thread_update_stats.start()

        thread_process = Thread(target=self._process_wrapper, name="controller-process_acks")
        thread_process.start()

        while True:
            time.sleep(1)