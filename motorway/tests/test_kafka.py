import unittest
from unittest.mock import patch, MagicMock

from confluent_kafka import Message as KafkaMessage

with patch('motorway.contrib.kafka.utils.reinitialize_consumer_on_error', lambda x: x):
    from motorway.contrib.kafka.ramps import KafkaRamp

class TestKafkaRamp(unittest.TestCase):
    @patch('motorway.contrib.kafka.ramps.Consumer')
    def get_kafka_ramp(self, mock_consumer_class, iterations=5):
        KafkaRamp.topic_name = 'test_topic'
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        kafka_ramp = KafkaRamp(consumer_thread_enabled=False, consume_iterations=iterations)
        kafka_ramp.consumer = mock_consumer
        return kafka_ramp

    @staticmethod
    def get_message_mock():
        mock_msg = MagicMock(spec=KafkaMessage)
        mock_msg.value.return_value = b'{ "data": "test_value" }'
        mock_msg.key.return_value = b'test_key'
        mock_msg.error.return_value = None
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 1
        return mock_msg

    def assert_commit_call_kwargs(self, method, **kwargs):
        """
        Because TopicPartion does not compare its instances correctly,
        we need to check the kwargs of the call.
        """
        for key, value in kwargs.items():
            called_value = getattr(method.call_args.kwargs['offsets'][0], key)
            self.assertEquals(called_value, value, f"Expected {key} to be {value}, but got {called_value}")


    def test_kafka_ramp_initialization(self):
        kafka_ramp = self.get_kafka_ramp()

        kafka_ramp.consumer.subscribe.assert_called_once()
        self.assertEquals(kafka_ramp.consumer.subscribe.call_args.args[0],['test_topic'])
        self.assertEquals(kafka_ramp.consumer.consume.call_count, 5)

    def test_consume_message(self):
        kafka_ramp = self.get_kafka_ramp(iterations=1)
        mock_msg = self.get_message_mock()
        kafka_ramp.consumer.consume.return_value = [mock_msg]

        with patch.object(kafka_ramp, 'insertion_queue') as mock_queue:
            with patch.object(kafka_ramp, 'uncompleted_ids') as mock_uncompleted_ids:
                kafka_ramp.consume(iterations=1)
                mock_queue.put.assert_called_once()
                mock_uncompleted_ids[0].add.assert_called_once_with(1)

    def test_next_message(self):
        kafka_ramp = self.get_kafka_ramp(iterations=1)
        mock_msg = self.get_message_mock()

        with patch.object(kafka_ramp.insertion_queue, 'get', return_value=mock_msg):
            motorway_message = next(kafka_ramp.next())
            self.assertEqual(motorway_message.grouping_value, 'test_key')
            self.assertEqual(motorway_message.content, {"data": 'test_value'})
            self.assertEqual(motorway_message.ramp_unique_id, '0-1')

    def test_success(self):
        kafka_ramp = self.get_kafka_ramp(iterations=1)
        commit = kafka_ramp.consumer.commit
        kafka_ramp.uncompleted_ids[0].add(1)

        kafka_ramp.success('0-1')

        self.assertFalse(kafka_ramp.uncompleted_ids[0])  # empty set
        self.assert_commit_call_kwargs(commit, topic='test_topic', partition=0, offset=2)  # offset + 1

    def test_success_multiple_uncompleted_ids(self):
        kafka_ramp = self.get_kafka_ramp(iterations=1)
        commit = kafka_ramp.consumer.commit
        kafka_ramp.uncompleted_ids[0].update({1, 2, 3})

        kafka_ramp.success('0-2')

        self.assertEquals({1, 3}, kafka_ramp.uncompleted_ids[0]) # 2 should be removed
        self.assert_commit_call_kwargs(commit, topic='test_topic', partition=0, offset=1)

        # now `1` finished processing, so we should commit `3`
        kafka_ramp.success('0-1')
        self.assertEquals({3}, kafka_ramp.uncompleted_ids[0])  # 1 should be removed as well
        self.assert_commit_call_kwargs(commit, topic='test_topic', partition=0, offset=3)

    def test_success_multiple_uncompleted_ids_edge_case(self):
        kafka_ramp = self.get_kafka_ramp(iterations=1)
        commit = kafka_ramp.consumer.commit
        kafka_ramp.uncompleted_ids[0].update({1, 2, 3})

        kafka_ramp.success('0-3')
        self.assert_commit_call_kwargs(commit, topic='test_topic', partition=0, offset=1)  # 1 is still uncompleted
        kafka_ramp.success('0-2')
        self.assert_commit_call_kwargs(commit, topic='test_topic', partition=0, offset=1) # 1 is still uncompleted
        kafka_ramp.success('0-1')
        # 1 is completed now, so ideally we should commit 4, but for simplicity we commit 2
        # because we don't know if 3 was completed or not at this point
        # on the next iteration we will commit 4
        self.assert_commit_call_kwargs(commit, topic='test_topic', partition=0, offset=2)

    def test_failed(self):
        kafka_ramp = self.get_kafka_ramp(iterations=1)
        kafka_ramp.uncompleted_ids[0].add(1)

        kafka_ramp.failed('0-1')

        self.assertFalse(kafka_ramp.uncompleted_ids[0])  # empty set

    @patch('motorway.contrib.kafka.utils.reinitialize_consumer_on_error', lambda x: x)
    def test_consume_throttle(self):
        class ThrottleException(Exception):
            pass

        kafka_ramp = self.get_kafka_ramp(iterations=0)
        kafka_ramp.MAX_UNCOMPLETED_ITEMS_PER_PARTITION = 2
        kafka_ramp._throttle = MagicMock(side_effect=ThrottleException)
        kafka_ramp.uncompleted_ids[0].update({1, 2})
        kafka_ramp.uncompleted_ids[1].update({3, 4, 5})
        kafka_ramp.uncompleted_ids[2].update({6, 7})

        with self.assertRaises(ThrottleException):
            kafka_ramp.consume(iterations=1)

    def test_consume_doesnt_throttle_below_limit(self):
        kafka_ramp = self.get_kafka_ramp(iterations=0)
        kafka_ramp.MAX_UNCOMPLETED_ITEMS_PER_PARTITION = 3
        kafka_ramp._throttle = MagicMock()
        kafka_ramp.uncompleted_ids[0].update({1, 2,})
        kafka_ramp.uncompleted_ids[1].update({3, 4, 5})
        kafka_ramp.uncompleted_ids[2].update({6, 7})

        kafka_ramp.consume(iterations=1)

        kafka_ramp._throttle.assert_not_called()

    def test_consume_throttle_global_limit(self):
        class ThrottleException(Exception):
            pass

        kafka_ramp = self.get_kafka_ramp(iterations=0)
        kafka_ramp.MAX_UNCOMPLETED_ITEMS_PER_PARTITION = 5  # High enough to not trigger per-partition limit
        kafka_ramp.MAX_TOTAL_UNCOMPLETED_ITEMS = 6  # Should trigger with 8 total items
        kafka_ramp._throttle = MagicMock(side_effect=ThrottleException)
        
        # Add items across multiple partitions, but stay under per-partition limit
        kafka_ramp.uncompleted_ids[0].update({1, 2})  # 2 items
        kafka_ramp.uncompleted_ids[1].update({3, 4})  # 2 items
        kafka_ramp.uncompleted_ids[2].update({5, 6, 7, 8})  # 4 items
        # Total: 8 items > MAX_TOTAL_UNCOMPLETED_ITEMS (6)

        with self.assertRaises(ThrottleException):
            kafka_ramp.consume(iterations=1)

    def test_consume_doesnt_throttle_below_global_limit(self):
        kafka_ramp = self.get_kafka_ramp(iterations=0)
        kafka_ramp.MAX_UNCOMPLETED_ITEMS_PER_PARTITION = 5  # High enough to not trigger per-partition limit
        kafka_ramp.MAX_TOTAL_UNCOMPLETED_ITEMS = 10  # High enough to not trigger global limit
        kafka_ramp._throttle = MagicMock()
        
        # Add items across multiple partitions, stay under both limits
        kafka_ramp.uncompleted_ids[0].update({1, 2})  # 2 items
        kafka_ramp.uncompleted_ids[1].update({3, 4})  # 2 items
        kafka_ramp.uncompleted_ids[2].update({5, 6})  # 2 items
        # Total: 6 items < MAX_TOTAL_UNCOMPLETED_ITEMS (10)

        kafka_ramp.consume(iterations=1)

        kafka_ramp._throttle.assert_not_called()
