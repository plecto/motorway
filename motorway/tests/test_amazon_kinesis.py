from unittest import TestCase
from boto.dynamodb2.exceptions import ItemNotFound
from motorway.contrib.amazon_kinesis.ramps import KinesisRamp
from mock import patch


class MockDynamoItem(dict):

    def __init__(self, **kwargs):
        super(MockDynamoItem, self).__init__(**kwargs)

    def save(self):
        pass  # This is a dictionary, so it is just updated straight away


class MockControlTable(object):
    def __init__(self, control_table_item_list):
        self.control_table_item_list = control_table_item_list

    def get_item(self, shard_id=None):
        for item in self.control_table_item_list:
            if item['shard_id'] == shard_id:
                return item
        raise ItemNotFound()

    def scan(self):
        for item in self.control_table_item_list:
            yield item

    def _put_item(self, item_data, expects=None):
        pass


class AmazonKinesisTestCase(TestCase):
    def get_kinesis_ramp(self, control_table_item_list=None, shards=None):
        with patch('boto.kinesis.connect_to_region', return_value=None) as mock_method:
            KinesisRamp.stream_name = "_unittest"
            KinesisRamp.heartbeat_timeout = 0  # Let's not care about waiting in this test
            kinesis_ramp = KinesisRamp(shard_threads_enabled=False)
            if not shards:
                shards = []
            if not control_table_item_list:
                control_table_item_list = [MockDynamoItem(
                    shard_id=shard_id,
                    checkpoint=0,
                    worker_id=i,
                    heartbeat=0,
                ) for i, shard_id in enumerate(shards)]
            for shard in shards:
                kinesis_ramp.uncompleted_ids[shard] = []
            kinesis_ramp.control_table = MockControlTable(control_table_item_list)
            return kinesis_ramp

    def test_can_claim_timeout(self):
        kinesis_ramp = self.get_kinesis_ramp(shards=['shard-1', 'shard-2'])
        self.assertTrue(
            kinesis_ramp.can_claim_shard("shard-1")
        )
        self.assertTrue(
            kinesis_ramp.can_claim_shard("shard-2")
        )

    def test_cannot_claim_timeout(self):
        kinesis_ramp = self.get_kinesis_ramp(shards=['shard-1',])
        def change_heartbeat(seconds):
            kinesis_ramp.control_table.get_item(shard_id='shard-1')['heartbeat'] += 1
        with patch('time.sleep', change_heartbeat) as mock_method:
            self.assertFalse(
                kinesis_ramp.can_claim_shard("shard-1")
            )

    def test_rebalance(self):
        control_table_item_list = [
            MockDynamoItem(
                shard_id='shard-1',
                checkpoint=0,
                worker_id=1,
                heartbeat=0,
            ),
            MockDynamoItem(
                shard_id='shard-2',
                checkpoint=0,
                worker_id=1,
                heartbeat=0,
            ),
        ]
        kinesis_ramp = self.get_kinesis_ramp(shards=['shard-1', 'shard-2'], control_table_item_list=control_table_item_list)

        def change_heartbeat(seconds):
            kinesis_ramp.control_table.get_item(shard_id='shard-1')['heartbeat'] += 1
            kinesis_ramp.control_table.get_item(shard_id='shard-1')['checkpoint'] = 1337
        with patch('time.sleep', change_heartbeat) as mock_method:
            self.assertTrue(
                kinesis_ramp.can_claim_shard("shard-1")
            )

    def test_claim_shard(self):
        kinesis_ramp = self.get_kinesis_ramp(shards=['shard-1', 'shard-2'])
        self.assertTrue(
            kinesis_ramp.claim_shard('shard-1')
        )

    def test_checkpoint_transfer(self):
        control_table_item_list = [
            MockDynamoItem(
                shard_id='shard-1',
                checkpoint=0,
                worker_id=1,
                heartbeat=0,
            ),
            MockDynamoItem(
                shard_id='shard-2',
                checkpoint=0,
                worker_id=1,
                heartbeat=0,
            ),
        ]
        kinesis_ramp_a = self.get_kinesis_ramp(shards=['shard-1', 'shard-2'], control_table_item_list=control_table_item_list)
        kinesis_ramp_b = self.get_kinesis_ramp(shards=['shard-1', 'shard-2'], control_table_item_list=control_table_item_list)
        kinesis_ramp_a.claim_shard("shard-1")
        kinesis_ramp_a.claim_shard("shard-2")

        self.assertEqual(kinesis_ramp_b.control_table.get_item('shard-1')['heartbeat'], 0)
        control_table_item_list[0]['checkpoint'] = 1337  # Simulate changing to DynamoDB entry
        kinesis_ramp_b.claim_shard('shard-1')
        self.assertEqual(kinesis_ramp_b.control_table.get_item('shard-1')['worker_id'], kinesis_ramp_b.worker_id)
        self.assertEqual(kinesis_ramp_b.control_table.get_item('shard-1')['checkpoint'], 1337)