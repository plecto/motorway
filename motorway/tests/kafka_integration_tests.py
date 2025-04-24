import os
import time
import unittest
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient


class KafkaIntegrationTest(unittest.TestCase):
    """
    Integration tests for Kafka producer and consumer, used to demonstrate functionality of Kafka
    and confluent_kafka library.
    """

    def setUp(self):
        self.topic_name = f'test_{uuid.uuid4()}'

    def get_kafka_producer(self):
        """
        Create a Kafka producer instance.
        """
        producer = Producer({
            'bootstrap.servers': os.environ.get('KAFKA_BROKER_URL', 'localhost:19092'),
            'queue.buffering.max.messages': 100000,  # Adjust to your needs
            'message.max.bytes': 1024 * 1024,  # Default is 1 MB, ensure it matches your cluster config
            'enable.idempotence': True  # Ensure delivery exactly once
        })
        return producer

    def get_kafka_consumer(self, additional_kwargs=None):
        additional_kwargs = additional_kwargs or {}
        consumer = Consumer({
            'bootstrap.servers': os.environ.get('KAFKA_BROKER_URL', 'localhost:19092'),
            'group.id': 'test_motorway_group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            **additional_kwargs,
        })
        return consumer

    def produce_messages(self, producer, num_messages=5):
        delivered_messages = []
        def _delivery_callback(err, msg, record):
            if err is not None:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Mess|age delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                delivered_messages.append(msg)

        for i in range(num_messages):
            record = {
                'key': f'test_key_{i}'.encode('utf-8'),
                'value': f'test_value_{i}'.encode('utf-8'),
            }
            producer.produce(
                topic=self.topic_name,
                key=record['key'],
                value=record['value'],
                callback=lambda err, msg, r=record: _delivery_callback(err, msg, r),
            )
            producer.poll(0)
        producer.flush()
        return delivered_messages

    def consume_messages(self, consumer, num_messages, commit=True):
        messages = consumer.consume(num_messages=num_messages, timeout=1)
        for message in messages:
            if message.error():
                print(f"Error consuming message: {message.error()}")
                continue

            print(f"Consumed message: {message.value().decode('utf-8')}")
            if commit:
                consumer.commit(message)

        return messages

    def test_commit_attempt_on_cancelled_consumer(self):
        """
        Test that the consumer fails to commit offsets after exceeding max.poll.interval.
        """
        producer = self.get_kafka_producer()
        delivered_messages = self.produce_messages(producer)

        def on_revoke(c, partitions):
            print(f"Partitions revoked: {partitions}")

        consumer = self.get_kafka_consumer(
            additional_kwargs={
                'session.timeout.ms': 6000,  # has to be larger than group.min.session.timeout.ms on the broker
                'max.poll.interval.ms': 7000,
            }
        )
        consumer.subscribe([self.topic_name], on_revoke=on_revoke)

        # Simulate consuming messages
        messages = consumer.consume(num_messages=3, timeout=1)

        # sleep so we exceed the max poll interval
        time.sleep(10)

        print('Trying to commit offsets after sleep')
        last_message = delivered_messages[-1]
        # raises KafkaException: KafkaError{code=UNKNOWN_MEMBER_ID,val=25,str="Commit failed: Broker: Unknown member"}
        with self.assertRaises(KafkaException):
            consumer.commit(offsets=[
                TopicPartition(self.topic_name, last_message.partition(), last_message.offset()),
            ],
                asynchronous=False
            )


    def test_consume_attempt_on_cancelled_consumer(self):
        """
        Test that the consumer fixes itself after exceeding max.poll.interval.
        """
        producer = self.get_kafka_producer()
        delivered_messages = self.produce_messages(producer, num_messages=10)
        consumer = self.get_kafka_consumer(
            additional_kwargs={
                'auto.offset.reset': 'earliest',  # starting from earliest
                'session.timeout.ms': 6000,  # has to be larger than group.min.session.timeout.ms on the broker
                'max.poll.interval.ms': 7000,
            }
        )
        consumer.subscribe([self.topic_name])

        # Simulate consuming messages
        messages = self.consume_messages(consumer, num_messages=1)
        self.assertEquals(len(messages), 1)
        self.assertIsNone(messages[0].error())

        # sleep so we exceed the max poll interval
        time.sleep(8)

        print('Trying to consume after exceeding max poll interval')
        messages = self.consume_messages(consumer, num_messages=11)
        self.assertEquals(len(messages), 10)  # 1 error and 9 valid messages
        self.assertEquals(messages[0].error().name(), '_MAX_POLL_EXCEEDED')
        for message in messages[1:]:
            self.assertIsNone(message.error())

    def tearDown(self):
        admin_client = AdminClient({
            'bootstrap.servers': os.environ.get('KAFKA_BROKER_URL', 'localhost:19092')
        })
        try:
            admin_client.delete_topics([self.topic_name], operation_timeout=30)
            print(f"Topic {self.topic_name} removed successfully.")
        except Exception as e:
            print(f"Failed to remove topic {self.topic_name}: {e}")
