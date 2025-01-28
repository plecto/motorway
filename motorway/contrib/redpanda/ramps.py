import json
import logging
import threading
from queue import Queue
from motorway.messages import Message
from motorway.ramp import Ramp
from motorway.contrib.redpanda.mixins import RedPandaMixin

logger = logging.getLogger(__name__)

class RedpandaRamp(Ramp, RedPandaMixin):
    topic_name = None
    group_id = None
    MAX_UNCOMPLETED_ITEMS = 3000

    def __init__(self, brokers, *args, **kwargs):
        super(RedpandaRamp, self).__init__(*args, **kwargs)
        assert self.topic_name, "Please define the attribute `topic_name` for your RedpandaRamp"
        assert self.group_id, "Please define the attribute `group_id` for your RedpandaRamp"

        self.brokers = brokers
        self.insertion_queue = Queue()
        self.uncompleted_ids = set()
        self.init_topic()

        # Start thread for consuming messages
        consume_thread = threading.Thread(target=self._consume_messages, daemon=True)
        consume_thread.start()

    def connection_parameters(self):
        """
        Override connection parameters for Kafka.
        """
        return {
            'bootstrap_servers': self.brokers,
            'group_id': self.group_id,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False
        }

    def _consume_messages(self):
        """
        Consume messages from the Redpanda topic and enqueue them.
        """
        for message in self.consume_messages(group_id=self.group_id):
            try:
                self.insertion_queue.put({
                    'key': message.get('key'),
                    'value': message.get('value'),
                    'offset': message.get('offset'),
                })
                self.uncompleted_ids.add(message.get('offset'))
            except Exception as e:
                logger.exception(f"Failed to process message: {e}")

    def next(self):
        """
        Fetch the next message from the queue.
        """
        msg = self.insertion_queue.get()
        try:
            yield Message(
                msg['offset'],
                msg['value'],
                grouping_value=msg['key']
            )
        except ValueError as e:
            logger.exception(e)

    def success(self, _id):
        """
        Acknowledge successful processing of a message.
        """
        if _id in self.uncompleted_ids:
            self.uncompleted_ids.remove(_id)
            # Commit the offset for the successfully processed message
            self.consumer.commit(asynchronous=False, offsets=[{
                'topic': self.topic_name,
                'partition': 0,  # Modify if using multiple partitions
                'offset': _id + 1
            }])

    def emit(self, value, key=None):
        """
        Emit a new message using the mixin's `send_message` method.
        """
        try:
            self.send_message({
                'key': key,
                'value': value
            })
        except Exception as e:
            logger.exception(f"Failed to emit message: {e}")

    def shutdown(self):
        """
        Cleanup on shutdown.
        """
        logger.info("Shutting down RedpandaRamp")
        self.consumer.close()
        self.producer.flush()