import json

from motorway.contrib.kafka.exceptions import KafkaMessageTooLarge

KAFKA_MESSAGE_MAXIMUM_SIZE = 1024 * 1024  # Default 1 MB (1,048,576 bytes).


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
