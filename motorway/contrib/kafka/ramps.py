import logging
import threading
import time
from collections import defaultdict
from queue import Queue

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka import Message as KafkaMessage

from motorway.contrib.kafka.mixins import KafkaMixin
from motorway.messages import Message
from motorway.ramp import Ramp

logger = logging.getLogger(__name__)

class KafkaRamp(Ramp, KafkaMixin):
    topic_name = None
    AUTO_OFFSET_RESET = 'latest'
    MAX_UNCOMPLETED_ITEMS = 3000
    GET_RECORDS_LIMIT = 1000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert self.topic_name, "Please define the attribute `topic_name` for your KafkaRamp"

        self.insertion_queue = Queue()
        self.uncompleted_ids = defaultdict(set)

        self.consumer = Consumer({
            **self.connection_parameters(),
        })
        thread = threading.Thread(
            target=self.consume,
            name=f"{self.__class__.__name__}-consume-{self.topic_name}",
        )
        logger.info("Starting Kafka consumer thread for topic %s", self.topic_name)
        thread.start()


    def consume(self):
        logger.info("Thread starting to consume for topic %s", self.topic_name)
        self.consumer.subscribe([self.topic_name])
        while True:
            # TODO: this will pause consumption from all assigned partitions if one partition has too many uncompleted items
            # This is not ideal, but it's a simple way to prevent the consumer from falling behind
            for partition in self.uncompleted_ids.keys():
                if len(self.uncompleted_ids[partition]) > self.MAX_UNCOMPLETED_ITEMS:
                    logger.warning("Too many uncompleted items, pausing consumption for 5 seconds")
                    time.sleep(5)
            messages = self.consumer.consume(num_messages=self.GET_RECORDS_LIMIT, timeout=5)
            for msg in messages:
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    logger.info("Waiting for messages...%s", self.topic_name)
                elif msg.error():
                    logger.error("ERROR: %s".format(msg.error()))
                else:
                    logger.debug("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                    self.insertion_queue.put(msg)
                    self.uncompleted_ids[msg.partition()].add(msg.offset())

    @property
    def group_id(self):
        """
        Important -- this is the group ID for the Kafka consumer group.
        Kafka uses this to keep track of which messages have been consumed.
        If you change this, you will re-consume all messages unless `auto.offset.reset' is set to 'latest'.
        """
        return 'motorway'

    @staticmethod
    def get_message_id(msg: KafkaMessage):
        partition_number = msg.partition() or 0
        return f"{partition_number}-{msg.offset()}"

    def connection_parameters(self):
        """
        Override connection parameters for Kafka.
        """
        return {
            **super().connection_parameters(),
            'group.id': self.group_id,
            'auto.offset.reset': self.AUTO_OFFSET_RESET,
            'enable.auto.commit': False,
        }

    def next(self):
        msg : KafkaMessage = self.insertion_queue.get()
        try:
            yield Message(
                self.get_message_id(msg),  # Unique ID
                self.decode_message(msg.value()),
                grouping_value=msg.key().decode('utf-8'),
            )
        except ValueError as e:
            logger.exception(e)

    def success(self, _id):
        """
        After a message has been successfully processed, commit the offset.
        Ideally it should be the oldest message in the uncompleted list, but it's not guaranteed.
        """
        logger.debug("Committing offset for %s", _id)
        partition_number, offset = map(int, _id.split('-'))
        self.uncompleted_ids[partition_number].remove(offset)
        # commit the oldest offset
        oldest_offset = min(self.uncompleted_ids[partition_number]) if self.uncompleted_ids[partition_number] else offset + 1
        self.consumer.commit(offsets=[TopicPartition(self.topic_name, partition_number, oldest_offset)])
