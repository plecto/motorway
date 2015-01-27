import datetime
import json
from threading import Thread
from flask import Flask, render_template, Response
from motorway.intersection import Intersection
from motorway.utils import DateTimeAwareJsonEncoder


class WebserverIntersection(Intersection):
    def __init__(self):
        super(WebserverIntersection, self).__init__()

        self.process_id_to_name = {}
        self.process_statistics = {}
        self.stream_consumers = {}
        self.failed_messages = {}

        app = Flask(__name__)

        @app.route("/")
        def index():
            return render_template("index.html")

        @app.route("/detail/<process>/")
        def detail(process):
            return render_template("detail.html", process=process)

        @app.route("/api/status/")
        def api_status():
            now = datetime.datetime.now()
            return Response(json.dumps(dict(
                sorted_process_statistics=sorted(
                    [(self.process_id_to_name.get(process_id, process_id), stats) for process_id, stats in self.process_statistics.items()],
                    key=lambda itm: itm[0]
                ),
                stream_consumers=self.stream_consumers,
                last_minutes=[(now - datetime.timedelta(minutes=i)).minute for i in range(0, 10)]
            ), cls=DateTimeAwareJsonEncoder), mimetype='application/json')

        @app.route("/api/detail/<process>/")
        def api_detail(process):
            return Response(json.dumps(dict(
                failed_messages=[msg[1] for msg in self.failed_messages.values() if msg[0] == process],
            ), cls=DateTimeAwareJsonEncoder), mimetype='application/json')

        p = Thread(target=app.run, name="motorway-webserver", kwargs=dict(
            port=5000,
            host="0.0.0.0",
        ))
        p.start()

    def process(self, message):
        self.process_id_to_name = message.content['process_id_to_name']
        self.process_statistics = message.content['process_statistics']
        self.stream_consumers = message.content['stream_consumers']
        self.failed_messages = message.content['failed_messages']
        yield