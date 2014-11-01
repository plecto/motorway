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

    def __init__(self, stream_consumers, ramp_socks, web_server=True):
        super(ControllerIntersection, self).__init__()
        self.stream_consumers = stream_consumers
        self.ramp_socks = ramp_socks
        self.messages = {}
        self.failed_messages = {}
        self.process_statistics = {}
        self.waiting_messages = {}

        for stream_dict in stream_consumers.values():
            for process in stream_dict['producers'] + stream_dict['consumers']:
                if not process in self.process_statistics:
                    self.process_statistics[process] = self.get_default_process_dict()


        if web_server:
            app = Flask(__name__)
            @app.route("/")
            def hello():
                return render_template("index.html")

            @app.route("/json/")
            def json_output():
                now = datetime.datetime.now()
                return Response(json.dumps(dict(
                    sorted_process_statistics=sorted(self.process_statistics.items(), key=lambda itm: itm[0]),
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
            if not message.ramp_unique_id in self.messages:  # Message just created
                self.messages[message.ramp_unique_id] = [process, message.ack_value, datetime.datetime.now()]  # Set the new value to ack value
            elif message.ack_value >= 0:  # Message processed
                self.messages[message.ramp_unique_id][1] ^= message.ack_value  # XOR the existing value
                # Update process information
                if self.messages[message.ramp_unique_id][1] == Message.SUCCESS:
                    original_process = self.messages[message.ramp_unique_id][0]
                    self.process_statistics[original_process]['success'] += 1
                    self.process_statistics[original_process]['histogram'][datetime.datetime.now().minute]['success_count'] += 1
                    self.success(message.ramp_unique_id, original_process.split("-")[0])
                    del self.messages[message.ramp_unique_id]
            elif message.ack_value == Message.FAIL:
                now = datetime.datetime.now()
                if message.ramp_unique_id in self.messages:
                    process, ack_value, start_time = self.messages[message.ramp_unique_id]
                del self.messages[message.ramp_unique_id]
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
        self.process_statistics[process]['failed'] += 1
        self.process_statistics[process]['histogram'][datetime.datetime.now().minute]['error_count'] += 1

        process_class = process.split("-")[0]

        if process_class not in self.ramp_socks:
            logging.debug("%s not in ramp_socks, probably a intersection which doesn't support feedback" % process_class)
        else:
            self.ramp_socks[process_class].send_json({
                'status': 'fail',
                'id': unique_id
            })

    def success(self, unique_id, process_class):
        if process_class not in self.ramp_socks:
            logging.debug("%s not in ramp_socks, probably a intersection which doesn't support feedback" % process_class)
        else:
            self.ramp_socks[process_class].send_json({
                'status': 'success',
                'id': unique_id
            })

    @classmethod
    def run(cls, input_stream, output_stream=None, stream_consumers=None, ramp_result_streams=None):
        context = zmq.Context()
        receive_sock = context.socket(zmq.PULL)
        receive_sock.connect(input_stream)
        set_timeouts_on_socket(receive_sock)

        ramp_socks = {}
        if ramp_result_streams:
            for ramp_class_name, ramp_stream in ramp_result_streams:
                ramp_socks[ramp_class_name] = context.socket(zmq.PUSH)
                ramp_socks[ramp_class_name].connect(ramp_stream)

        self = cls(stream_consumers, ramp_socks)

        setproctitle("data-pipeline: %s" % cls.__name__)
        logger.info("Running %s" % cls.__name__)

        while True:
            self._process(receive_sock, output_stream, None)
            self.update()
