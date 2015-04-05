import json
import boto.kinesis
from motorway.intersection import Intersection


class KinesisInsertIntersection(Intersection):
    stream_name = None

    def __init__(self):
        super(KinesisInsertIntersection, self).__init__()
        self.conn = boto.kinesis.connect_to_region(**self.connection_parameters())
        assert self.stream_name, "Please define attribute stream_name on your KinesisInsertIntersection"

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }

    def process(self, message):
        self.conn.put_record(
            self.stream_name,
            json.dumps(message.content),
            message.grouping_value
        )
        self.ack(message)
        yield
