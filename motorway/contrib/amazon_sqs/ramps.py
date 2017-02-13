from motorway.contrib.amazon_sqs.utils import SQSJSONMessage
from motorway.messages import Message
from motorway.ramp import Ramp
import boto.sqs


class SQSRamp(Ramp):
    queue_name = None
    sqs_message_class = SQSJSONMessage
    json_group_key = None

    def __init__(self, *args, **kwargs):
        super(SQSRamp, self).__init__(*args, **kwargs)
        conn = boto.sqs.connect_to_region(**self.connection_parameters())
        assert self.queue_name, "Please define attribute queue_name on your SQSRamp"
        self.queue = conn.create_queue(self.queue_name)
        self.queue.set_message_class(self.sqs_message_class)
        self.messages = {}

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }

    def next(self):
        for msg in self.queue.get_messages(num_messages=10, wait_time_seconds=5, visibility_timeout=10*60):
            # Gets max 10 messages, waiting for max 5 seconds to receive them and blocks other from receiving it for 10m
            self.messages[msg.id] = msg
            if self.json_group_key:
                body = msg.get_body()
                yield Message(msg.id, body, grouping_value=body[self.json_group_key])
            else:
                yield Message(msg.id, msg.get_body())

    def success(self, _id):
        if _id in self.messages:
            self.queue.delete_message(self.messages[_id])  # TODO: Do this on ack
            del self.messages[_id]
