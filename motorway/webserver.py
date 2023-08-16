import datetime
import json
import logging
import socket
from threading import Thread

import zmq
from flask import Flask, render_template, Response
from isodate import parse_duration

from motorway.intersection import Intersection
from motorway.utils import DateTimeAwareJsonEncoder

logger = logging.getLogger(__name__)


class WebserverIntersection(Intersection):
    """
    Simple Flask webserver that exposes information sent from the controller(s).

    It groups processes by name and further more sets a "status" for the different processes, based on
    how busy they are.
    """

    send_control_messages = False

    def get_last_minutes(self, minute_count=10):
        now = datetime.datetime.now()
        return [(now - datetime.timedelta(minutes=i)).minute for i in range(0, minute_count)]

    def __init__(self, *args, **kwargs):
        super(WebserverIntersection, self).__init__(*args, **kwargs)

        self.process_statistics = {}
        self.stream_consumers = {}
        self.failed_messages = {}
        self.groups = {}

        app = Flask(__name__)

        @app.route("/")
        def index():
            return render_template("index.html")

        @app.route("/app.js")
        def js():
            return Response(render_template("app.js"), mimetype='application/javascript')

        @app.route("/style.css")
        def css():
            return Response(render_template("style.css"), mimetype='text/css')

        @app.route("/detail/<process>/")
        def detail(process):
            return render_template(
                "detail.html",
                process=process,
                hostname=socket.gethostname(),
                messages_being_processed=self._get_messages_being_processed_for_process(process),
                process_stats=self.process_statistics.get(process),
                failed_messages=reversed(sorted([msg for msg in self.failed_messages.values() if
                                 msg[1] == process], key=lambda itm: itm[0])[-20:]),

            )

        @app.route("/api/status/")
        def api_status():
            return Response(json.dumps(dict(
                groups=self.groups,
                last_minutes=self.get_last_minutes()
            ), cls=DateTimeAwareJsonEncoder), mimetype='application/json', headers={
                'Access-Control-Allow-Origin': '*'
            })

        # @app.route("/api/detail/<process>/")
        # def api_detail(process):
        #     return Response(json.dumps(dict(
        #         failed_messages=[msg[1] for msg in self.failed_messages.values() if self.process_id_to_name[msg[0]] == process],
        #     ), cls=DateTimeAwareJsonEncoder), mimetype='application/json')

        p = Thread(target=app.run, name="motorway-webserver", kwargs=dict(
            port=5000,
            host="0.0.0.0",
        ))
        p.start()

    def _get_messages_being_processed_for_process(self, process_uuid):
        report_socket_address = self.process_id_to_report_address.get(process_uuid)
        messages_being_processed = []
        if report_socket_address:
            messages_being_processed = self._get_messages_being_processed_from_socket(report_socket_address)

        return messages_being_processed

    def _get_messages_being_processed_from_socket(self, socket_address):
        context = zmq.Context()
        report_socket = context.socket(zmq.REQ)

        report_socket.connect(socket_address)
        report_socket.send_string('')
        messages_being_processed_json = report_socket.recv_json()

        return [msg for msg in json.loads(messages_being_processed_json)]

    def process(self, message):
        self.process_statistics = message.content['process_statistics']
        self.stream_consumers = message.content['stream_consumers']
        self.failed_messages = message.content['failed_messages']

        for process_id, stats in self.process_statistics.items():
            if process_id in self.process_id_to_name:

                stats['state'] = 'available'
                if stats['waiting'] > 0:
                    stats['state'] = 'busy'
                    processed_last_few_minutes = 0
                    for minute in self.get_last_minutes(3):
                        processed_last_few_minutes += stats['histogram'][str(minute)]['processed_count']
                    if stats['waiting'] > processed_last_few_minutes:
                        stats['state'] = 'overloaded'

                group_name = self.process_id_to_name[process_id].split('-')[0]
                new_stats = self.groups.get(group_name, {'processes': {}.copy()}.copy())
                new_stats['processes'][process_id] = stats
                self.groups[group_name] = new_stats

        for group in self.groups.values():
            group['waiting'] = sum([process['waiting'] for process in group['processes'].values()])
            group['time_taken'] = datetime.timedelta()
            group['histogram'] = {str(minute): {'error_count': 0, 'success_count': 0, 'timeout_count': 0, 'processed_count': 0}.copy() for minute in range(0, 60)}.copy()
            for process_id, process in group['processes'].items():

                # Remove stale processes (those no longer in the connection thread)
                if process_id not in self.process_id_to_name:
                    del group['processes'][process_id]
                    continue

                # Calculate statistics on the active processes
                group['time_taken'] += parse_duration(process['time_taken']) or datetime.timedelta(seconds=0)
                for minute, histogram_dict in process.get('histogram').items():
                    group['histogram'][minute]['error_count'] += histogram_dict['error_count']
                    group['histogram'][minute]['success_count'] += histogram_dict['success_count']
                    group['histogram'][minute]['timeout_count'] += histogram_dict['timeout_count']
                    group['histogram'][minute]['processed_count'] += histogram_dict['processed_count']
            group['frequency'] = sum([sum(process['frequency'].values()) for process in group['processes'].values()]) or 1  # Fallback to at least one, otherwise division fails below

            group['avg_time_taken'] = group['time_taken'] / group['frequency'] / len(group['processes']) if len(group['processes']) else 0

        yield