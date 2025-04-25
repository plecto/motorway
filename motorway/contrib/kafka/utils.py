import functools
import json
import logging
import time

from motorway.contrib.kafka.exceptions import KafkaMessageTooLarge

KAFKA_MESSAGE_MAXIMUM_SIZE = 1024 * 1024  # Default 1 MB (1,048,576 bytes).
ramps_logger = logging.getLogger('motorway.contrib.kafka.ramps')


def kafka_encode_to_json(value, encoder_class=None):
    """
    Encodes a value into a JSON string and ensures it does not exceed Kafka's message size limit.
    
    :param value: The Python object to encode.
    :param encoder_class: A custom JSON encoder class (optional).
    :raises KafkaMessageTooLarge: If the message size exceeds the allowed limit.
    :return: JSON-encoded string.
    """
    json_string = json.dumps(value, cls=encoder_class)
    message_size = len(json_string.encode('utf-8'))  # Get message size in bytes
    
    if message_size > KAFKA_MESSAGE_MAXIMUM_SIZE:
        raise KafkaMessageTooLarge(
            f"Message size of {message_size}B exceeds the maximum Kafka's message size of "
            f"{KAFKA_MESSAGE_MAXIMUM_SIZE}B"
        )
    
    return json_string


def reinitialize_consumer_on_error(method):
    """
    Decorator to reinitialize the Kafka consumer on error.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        while True:
            try:
                ramps_logger.info("Starting Kafka consumer...")
                return method(self, *args, **kwargs)
            except Exception as e:
                ramps_logger.error("Error in Kafka consumer: %s. Reinitializing consumer...", e)
                ramps_logger.exception(e)
                self.consumer.close()  # Close the existing consumer
                time.sleep(5)
                continue

    return wrapper