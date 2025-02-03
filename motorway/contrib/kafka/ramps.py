import logging
import threading
from queue import Queue

import boto3
from confluent_kafka import Consumer
from confluent_kafka import Message as KafkaMessage

from motorway.contrib.kafka.mixins import KafkaMixin
from motorway.messages import Message
from motorway.ramp import Ramp

logger = logging.getLogger(__name__)

class KafkaRamp(Ramp, KafkaMixin):
    topic_name = None
    heartbeat_timeout = 30  # Wait 10 seconds for a heartbeat update, or kill it
    MAX_UNCOMPLETED_ITEMS = 3000
    GET_RECORDS_LIMIT = 1000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert self.topic_name, "Please define the attribute `topic_name` for your KafkaRamp"

        self.insertion_queue = Queue()
        self.uncompleted_ids = set()

        self.consumer = Consumer({
            **self.connection_parameters(),
        })
        t = threading.Thread(target=self.consume, name="%s-%s" % (self.__class__.__name__, 'consume'))
        t.start()
        # try:
        #     self.consume()
        # except Exception as e:
        #     logger.exception(e)
        #     print('Closing consumer...')
        #     self.consumer.close()

    def consume(self):
        # TODO: do we need to worry about this?
        # if the shard was claimed by another worker, break out of the loop
        #if not control_record['worker_id'] == self.worker_id:
        self.consumer.subscribe([self.topic_name])
        while True:
            messages = self.consumer.consume(num_messages=self.GET_RECORDS_LIMIT)
            for msg in messages:
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting...")
                elif msg.error():
                    print("ERROR: %s".format(msg.error()))
                else:
                    # Extract the (optional) key and value, and print.
                    print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                    self.insertion_queue.put(msg)
                    self.consumer.commit(message=msg)

    @property
    def group_id(self):
        """
        Important -- this is the group ID for the Kafka consumer group.
        Kafka uses this to keep track of which messages have been consumed.
        If you change this, you will re-consume all messages unless `auto.offset.reset' is set to 'latest'.
        """
        return 'motorway'

    def connection_parameters(self):
        """
        Override connection parameters for Kafka.
        """
        return {
            **super().connection_parameters(),
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',  # TODO: make sure this is right
            'enable.auto.commit': False,
        }

    def next(self):
        msg : KafkaMessage = self.insertion_queue.get()
        partition_number = msg.partition() or 0
        try:
            yield Message(
                f'{partition_number}-{msg.offset()}',  # Unique ID
                self.decode_message(msg.value()),
                grouping_value=msg.key().decode('utf-8'),
            )
        except ValueError as e:
            logger.exception(e)

    #
    # def dynamodb_connection_parameters(self):
    #     """Later to be replaced by Redis"""
    #     return {
    #         'region_name': 'eu-west-1',
    #         'service_name': 'dynamodb',
    #         # Add this or use ENV VARS
    #         # 'aws_access_key_id': '',
    #         # 'aws_secret_access_key': '',
    #     }
    #
    # def get_control_table_name(self):
    #     return 'pipeline-control-%s' % self.topic_name
    #
