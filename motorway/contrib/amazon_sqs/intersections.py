from motorway.intersection import Intersection
import boto3
from utils import sqs_encode_to_json


class SQSInsertIntersection(Intersection):
    queue_name = None

    def __init__(self, **kwargs):
        super(SQSInsertIntersection, self).__init__(**kwargs)
        self.sqs = boto3.resource(**self.connection_parameters())
        assert self.queue_name, "Please define attribute queue_name on your SQSRamp"
        self.queue = self.sqs.create_queue(QueueName=self.queue_name)

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            'service_name': 'sqs'
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }

    def process(self, message):
        self.queue.send_message(MessageBody=sqs_encode_to_json(message.content))
        self.ack(message)
        yield
