import json
from confluent_kafka import Producer
from motorway.decorators import batch_process
from motorway.intersection import Intersection
from time import sleep
import logging
from motorway.contrib.redpanda.mixins import RedPandaMixin


logger = logging.getLogger(__name__)


class RedpandaInsertIntersection(Intersection, RedPandaMixin):
    topic_name = None

    def __init__(self, **kwargs):
        super(RedpandaInsertIntersection, self).__init__(**kwargs)
        assert self.topic_name, "Please define the attribute 'topic_name' on your RedpandaInsertIntersection"
        self.producer = self.create_producer()

    def create_producer(self):
        """
        Create and configure a Kafka producer for Redpanda.
        Modify the bootstrap.servers with your Redpanda cluster endpoints.
        """
        return Producer({
            'bootstrap.servers': 'redpanda:9092',  # Replace with your Redpanda cluster endpoint
            'queue.buffering.max.messages': 100000,  # Adjust to your needs
            'message.max.bytes': 1024 * 1024,  # Default is 1 MB, ensure it matches your cluster config
            'enable.idempotence': True  # Ensure delivery exactly once
        })

    @batch_process(limit=500, wait=1)
    def process(self, messages):
        """
        Process messages in batches for Redpanda.
        Each batch can contain up to 500 records, but Redpanda allows larger limits per partition.
        The function retries failed deliveries and logs errors for further inspection.
        """
        records = []
        for message in messages:
            try:
                key = message.grouping_value.encode('utf-8')
                value = json.dumps(message.content).encode('utf-8')
                records.append({'key': key, 'value': value, 'message': message})
            except Exception as e:
                logger.error(f"Failed to serialize message: {message}. Error: {e}")
                self.fail(message)

        while records:
            for record in records:
                try:
                    self.producer.produce(
                        topic=self.topic_name,
                        key=record['key'],
                        value=record['value'],
                        callback=lambda err, msg, r=record: self._delivery_callback(err, msg, r)
                    )
                except BufferError:
                    logger.warning("Producer buffer is full, retrying...")
                    sleep(1)
                    continue

            # Poll to ensure delivery
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
