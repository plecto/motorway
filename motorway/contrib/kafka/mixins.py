from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json


class KafkaMixin:
    def init_topic(self):
        assert self.topic_name, "Please define attribute 'topic_name' on your class"
        # Optionally validate or create the topic (this may require external tooling like `rpk` or admin API)
        self.producer = KafkaProducer(**self.connection_parameters())

    def send_message(self, message):
        """
        Send a message to the specified topic.
        """
        try:
            encoded_message = self.encode_message(message)
            self.producer.send(self.topic_name, value=encoded_message)
            self.producer.flush()  # Ensure the message is sent immediately
        except KafkaError as e:
            print(f"Failed to send message: {e}")
            raise

    def consume_messages(self, group_id, auto_offset_reset="earliest"):
        """
        Consume messages from the specified topic.
        """
        consumer = KafkaConsumer(
            self.topic_name,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            **self.connection_parameters()
        )
        for message in consumer:
            yield self.decode_message(message.value)

    def encode_message(self, message):
        """
        Encode the message as JSON.
        """
        return json.dumps(message).encode('utf-8')

    def decode_message(self, message):
        """
        Decode the message from JSON.
        """
        return json.loads(message)

    def connection_parameters(self):
        """
        Connection parameters for Kafka.
        """
        return {
            'bootstrap.servers': 'redpanda:9092'
        }
