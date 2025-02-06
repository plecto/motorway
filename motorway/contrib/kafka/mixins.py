import json


class KafkaMixin:

    def encode_message(self, message):
        """
        Encode the message as JSON.
        """
        return json.dumps(message).encode('utf-8')

    def decode_message(self, message):
        """
        Decode the message from JSON.
        """
        return json.loads(message.decode('utf-8'))

    def connection_parameters(self):
        """
        Connection parameters for Kafka.
        """
        return {
            'bootstrap.servers': 'redpanda:9092'
        }
