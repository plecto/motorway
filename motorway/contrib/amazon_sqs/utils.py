import json
from boto.sqs.message import Message
from motorway.contrib.amazon_sqs.exceptions import SQSMessageToLarge

SQS_MESSAGE_MAXIMUM_SIZE = 262144


class SQSJSONMessage(Message):
    def encode(self, value):
        s = json.dumps(value)
        message_size = len(s.encode('utf-8'))
        if message_size > SQS_MESSAGE_MAXIMUM_SIZE:
            raise SQSMessageToLarge('message was %s in bytes and cant be bigger than %s' % (message_size, SQS_MESSAGE_MAXIMUM_SIZE))
        return s

    def decode(self, value):
        return json.loads(value)
