from motorway.contrib.amazon_sqs.mixins import SQSMixin
from motorway.intersection import Intersection
import boto3
from .utils import sqs_encode_to_json


class SQSInsertIntersection(Intersection, SQSMixin):
    queue_name = None

    def __init__(self, **kwargs):
        super(SQSInsertIntersection, self).__init__(**kwargs)
        self.sqs = boto3.resource(**self.connection_parameters())
        self.init_queue()

    def process(self, message):
        self.queue.send_message(MessageBody=sqs_encode_to_json(message.content))
        self.ack(message)
        yield
