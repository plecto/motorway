class KafkaMessageTooLarge(Exception):
    """Exception raised when a message exceeds the maximum size allowed by Kafka."""
    pass