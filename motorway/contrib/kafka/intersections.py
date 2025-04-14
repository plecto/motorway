import json

from confluent_kafka import Producer

from motorway.contrib.kafka.mixins import KafkaMixin
from motorway.decorators import batch_process
from motorway.intersection import Intersection
from time import sleep
import logging


logger = logging.getLogger(__name__)


class KafkaInsertIntersection(Intersection, KafkaMixin):
    topic_name = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert self.topic_name, "Please define the attribute 'topic_name' on your KafkaInsertIntersection"
        self.producer = self.create_producer()

    def create_producer(self):
        """
        Create and configure a Kafka producer for Kafka.
        Modify the bootstrap.servers with your Kafka cluster endpoints.
        """
        return Producer({
            **self.connection_parameters(),
            'queue.buffering.max.messages': 100000,  # Adjust to your needs
            'message.max.bytes': 1024 * 1024,  # Default is 1 MB, ensure it matches your cluster config
            'enable.idempotence': True  # Ensure delivery exactly once
        })

    @batch_process(limit=500, wait=1)
    def process(self, messages):
        """
        Process messages in batches for Kafka.
        Each batch can contain up to 500 records, but Kafka allows larger limits per partition.
        The function retries failed deliveries and logs errors for further inspection.
        """
        records = []
        for message in messages:
            try:
                value = json.dumps(message.content).encode('utf-8')
            except TypeError as e:
                logger.error(f"Failed to serialize message: {message}. Error: {e}")
                self.fail(message)
            else:
                key = message.grouping_value.encode('utf-8')
                records.append({'key': key, 'value': value, 'message': message})

        while records:
            for record in records:
                try:
                    self.producer.produce(
                        topic=self.topic_name,
                        key=record['key'],
                        value=record['value'],
                        callback=lambda err, msg, r=record: self._delivery_callback(err, msg, r),
                    )
                    self.producer.poll(0)  # Process events to handle delivery reports (previous messages).
                except BufferError:
                    logger.warning("Producer buffer is full, retrying...")
                    self.producer.poll(10)
                    continue

            # flush to ensure delivery
            self.producer.flush()
            records = [r for r in records if 'ack' not in r]

            if records:
                logger.warning(f"{len(records)} records failed, retrying...")
                sleep(1)

        yield

    def _delivery_callback(self, err, msg, record):
        """
        Callback to handle message delivery results.
        If there's an error, retry or fail the message.
        """
        if err is not None:
            logger.error(f"Delivery failed for record {record['message']}: {err}")
            self.fail(record['message'])
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
            self.ack(record['message'])
            record['ack'] = True
