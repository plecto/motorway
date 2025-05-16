import logging
import threading
import time
from collections import defaultdict
from queue import Queue

from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka import Message as KafkaMessage

from motorway.contrib.kafka.mixins import KafkaMixin
from motorway.contrib.kafka.utils import reinitialize_consumer_on_error
from motorway.messages import Message
from motorway.ramp import Ramp

logger = logging.getLogger(__name__)

class KafkaRamp(Ramp, KafkaMixin):
    """
    Implementation of a Kafka ramp relying on broker based Kafka consumer groups.

    The first time we call consumer.consume() (polling loop) it is actually joining the consumer group,
    and receiving a partition assignment.

    If a rebalance occurs, it will be managed within the polling loop along with any associated callbacks.
    As a result, most issues with the consumer or its listener callbacks are likely to manifest
    as exceptions raised by consumer.consume() or in message.error().
    """
    topic_name = None
    AUTO_OFFSET_RESET = 'latest'
    MAX_UNCOMPLETED_ITEMS = 10_000
    GET_RECORDS_LIMIT = 3000
    THROTTLE_SECONDS = 5

    def __init__(self, consumer_thread_enabled=True, consume_iterations=None, **kwargs):
        super().__init__(**kwargs)
        assert self.topic_name, "Please define the attribute `topic_name` for your KafkaRamp"
        self.logger = logger

        self.insertion_queue = Queue()
        self.uncompleted_ids = defaultdict(set)
        self.commited_offsets = defaultdict(lambda: 0)

        self.consumer = Consumer({
            **self.connection_parameters(),
        })
        if consumer_thread_enabled:
            self.start_consumer_thread()
        else:
            self.consume(iterations=consume_iterations)
            logger.info('Skipping consumer thread for topic %s', self.topic_name)

    def start_consumer_thread(self):
        thread = threading.Thread(
            target=self.consume,
            name=f"{self.__class__.__name__}-consume-{self.topic_name}",
        )
        logger.info("Starting Kafka consumer thread for topic %s", self.topic_name)
        thread.start()

    def _get_blocked_partitions(self):
        return [
            str(partition)
            for partition in self.uncompleted_ids.keys()
            if len(self.uncompleted_ids[partition]) > self.MAX_UNCOMPLETED_ITEMS
        ]

    def _too_many_uncompleted_items(self):
        """
        Pause consumption if we have too many uncompleted items.
        Because we use kafka built-in balancing of consumer group, we need to pause consumption
        from all assigned partitions even if only one of them has too many uncompleted items.

        We actually consume 1 message every time we call _throttle() to avoid exceeding the max poll interval
        (Kafka doesn't allow calling poll without getting messages).
        """
        return any(self._get_blocked_partitions())

    def _throttle(self):
        blocked_partitions = ', '.join(self._get_blocked_partitions())
        logger.warning("Too many uncompleted items for partitions %s, pausing consumption for %d seconds", blocked_partitions, self.THROTTLE_SECONDS)
        time.sleep(self.THROTTLE_SECONDS)
        # Consume just one message to avoid exceeding the max poll interval
        # and to allow the consumer to commit
        msg = self.consumer.poll(0)
        self._process_message(msg)

    @reinitialize_consumer_on_error
    def consume(self, iterations=None):
        """
        Consume messages from Kafka and put them in the insertion queue.

        :param iterations: Number of iterations to consume before stopping, useful in testing
        """
        logger.info("Thread starting to consume for topic %s", self.topic_name)
        self.consumer.subscribe([self.topic_name], on_assign=self.on_assign, on_revoke=self.on_revoke)
        current_iteration = 0

        while iterations is None or current_iteration < iterations:
            while self._too_many_uncompleted_items():
                self._throttle()
            messages = self.consumer.consume(num_messages=self.GET_RECORDS_LIMIT, timeout=1)
            self.log_message_consumption(messages, current_iteration)
            for msg in messages:
                self._process_message(msg)

            self.logging_hook(current_iteration)

            current_iteration += 1

    def _process_message(self, msg: KafkaMessage):
        if msg is None:
            logger.info("Waiting for messages...%s", self.topic_name)
        elif msg.error():
            logger.exception(KafkaException(msg.error()))
        else:
            logger.debug("Consumed message from topic %s: key = %s value = %s",
                         msg.topic(), msg.key().decode('utf-8')[:12], msg.value().decode('utf-8')[:12])
            self.insertion_queue.put(msg)
            self.uncompleted_ids[msg.partition()].add(msg.offset())

    @property
    def group_id(self):
        """
        Important -- this is the group ID for the Kafka consumer group.
        Kafka uses this to keep track of which messages have been consumed.
        If you change this, you will re-consume all messages unless `auto.offset.reset' is set to 'latest'.
        """
        return f'motorway-{self.topic_name}'

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
        We always commit the oldest uncompleted offset for the partition, so that we don't skip any messages
        when processing is stopped and started again.
        Processing starts from the latest commited offset, so in our case it would start from the oldest uncompleted offset for the partition.
        """
        partition_number, offset = map(int, _id.split('-'))
        if offset not in self.uncompleted_ids[partition_number]:
            logger.warning("Offset %s not in uncompleted ids for partition %s", offset, partition_number)
            return
        self.uncompleted_ids[partition_number].remove(offset)
        # commit the oldest offset
        oldest_offset = min(self.uncompleted_ids[partition_number]) if self.uncompleted_ids[partition_number] else offset + 1
        if oldest_offset > self.commited_offsets[partition_number]:  # only commit if the offset is newer
            self.commited_offsets[partition_number] = oldest_offset
            topic_partition = TopicPartition(self.topic_name, partition_number, oldest_offset)
            self.consumer.commit(offsets=[topic_partition], asynchronous=True)
            logger.debug("Committing offset for topic %s: %s", self.topic_name, _id)

    @staticmethod
    def on_assign(consumer, partitions):
        formatted_partitions = [
            f"Topic: {p.topic}, Partition: {p.partition}, Offset: {p.offset}, Error: {p.error}"
            for p in partitions
        ]
        logger.info("Partitions assigned:\n%s", "\n".join(formatted_partitions))

    @staticmethod
    def on_revoke(consumer, partitions):
        formatted_partitions = [
            f"Topic: {p.topic}, Partition: {p.partition}, Offset: {p.offset}, Error: {p.error}"
            for p in partitions
        ]
        logger.info("Partitions revoked:\n%s", "\n".join(formatted_partitions))

    def log_message_consumption(self, messages, current_iteration):
        if len(messages) or current_iteration % 100 == 0:
            logger.debug("Consumed %s messages from topic %s", len(messages), self.topic_name)

    def logging_hook(self, current_iteration):
        """Custom hook to override in the application invoked from the main consume loop."""
        pass
