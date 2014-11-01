import json
from boto.sqs.message import Message


class SQSJSONMessage(Message):
    def encode(self, value):
        return json.dumps(value)

    def decode(self, value):
        return json.loads(value)