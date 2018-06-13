from motorway.messages import Message
from motorway.ramp import Ramp
import boto3
import json


class SQSRamp(Ramp):
    queue_name = None
    json_group_key = None

    def __init__(self, *args, **kwargs):
        super(SQSRamp, self).__init__(*args, **kwargs)
        self.sqs = boto3.resource(**self.connection_parameters())
        assert self.queue_name, "Please define attribute queue_name on your SQSRamp"
        self.queue = self.sqs.create_queue(QueueName=self.queue_name)
        self.messages = {}

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            'service_name': 'sqs'
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }

    def next(self):
        for msg in self.queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5, VisibilityTimeout=10*60):
            # Gets max 10 messages, waiting for max 5 seconds to receive them and blocks other from receiving it for 10m
            self.messages[msg.message_id] = msg.receipt_handle  # we need the receipt_handle to delete the message and we always want to store the latest one, according to the sqs docs
            body = json.loads(msg.body)
            if self.json_group_key:
                yield Message(msg.message_id, body, grouping_value=body[self.json_group_key])
            else:
                yield Message(msg.message_id, body)

    def success(self, _id):
        if _id in self.messages:
            self.queue.delete_messages(Entries=[{'Id': _id, 'ReceiptHandle': self.messages[_id]}])  # TODO: Do this on ack
            del self.messages[_id]
