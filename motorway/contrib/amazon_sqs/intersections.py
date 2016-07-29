import boto.sqs
from motorway.contrib.amazon_sqs.utils import SQSJSONMessage
from motorway.intersection import Intersection


class SQSInsertIntersection(Intersection):
    queue_name = None
    sqs_message_class = SQSJSONMessage

    def __init__(self, **kwargs):
        super(SQSInsertIntersection, self).__init__(**kwargs)
        conn = boto.sqs.connect_to_region(**self.connection_parameters())
        assert self.queue_name, "Please define attribute queue_name on your SQSRamp"
        self.queue = conn.create_queue(self.queue_name)

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }

    def process(self, message):
        self.queue.write(
            self.sqs_message_class(body=message.content)
        )
        self.ack(message)
        yield
