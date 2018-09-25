from unittest import TestCase
from motorway.contrib.amazon_kinesis.ramps import KinesisRamp, NoItemsReturned
from mock import patch


class MockDynamoItem(dict):

    def __init__(self, **kwargs):
        super(MockDynamoItem, self).__init__(**kwargs)

    def save(self):
        pass  # This is a dictionary, so it is just updated straight away


class MockControlTable(object):
    def __init__(self, control_table_item_list):
        self.control_table_item_list = control_table_item_list

    def get_item(self, Key=None):
        for item in self.control_table_item_list:
            if item['shard_id'] == Key['shard_id']:
                return {'Item': item}  # a kinesis respons contains a dictionary with lots of metadata and and Item element if any items where found
        raise NoItemsReturned()

    def scan(self):
        return {'Items': self.control_table_item_list}

    def put_item(self, Item, ConditionExpression):
        for i, item in enumerate(self.control_table_item_list):
            if item['shard_id'] == Item['shard_id']:
                self.control_table_item_list[i] = Item


class AmazonKinesisTestCase(TestCase):
    def get_kinesis_ramp(self, control_table_item_list=None, shards=None):
        with patch('boto3.client', return_value=None) as mock_method:
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

            if control_table_item_list:
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
            kinesis_ramp.control_table.get_item(Key={'shard_id':'shard-1'})['Item']['heartbeat'] += 1
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
            kinesis_ramp.control_table.get_item(Key={'shard_id': 'shard-1'})['Item']['heartbeat'] += 1
            kinesis_ramp.control_table.get_item(Key={'shard_id': 'shard-1'})['Item']['checkpoint'] = 1337
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

        self.assertEqual(kinesis_ramp_b.control_table.get_item(Key={'shard_id': 'shard-1'})['Item']['heartbeat'], 0)
        control_table_item_list[0]['checkpoint'] = 1337  # Simulate changing to DynamoDB entry
        kinesis_ramp_b.claim_shard('shard-1')
        self.assertEqual(kinesis_ramp_b.control_table.get_item(Key={'shard_id': 'shard-1'})['Item']['worker_id'], kinesis_ramp_b.worker_id)
        self.assertEqual(kinesis_ramp_b.control_table.get_item(Key={'shard_id': 'shard-1'})['Item']['checkpoint'], 1337)

    def test_re_balance(self):
        """
        test for re-balancing when we have 3 ramps and 10 shards
        :return:
        """
        shards = []
        control_table_item_list = []
        kinesis_ramp1 = self.get_kinesis_ramp()
        kinesis_ramp2 = self.get_kinesis_ramp()
        kinesis_ramp3 = self.get_kinesis_ramp()
        worker_id = kinesis_ramp1.worker_id
        for i in xrange(1, 11):
            shard_id = "shard-%s" % str(i)
            shards.append(shard_id)
            if i == 11:
                # leave one shard to be claimed
                continue
            if i == 4:
                worker_id = kinesis_ramp2.worker_id
            elif i == 7:
                worker_id = kinesis_ramp3.worker_id
            # assign each worker 3 shards
            control_table_item_list.append(MockDynamoItem(
                shard_id=shard_id,
                checkpoint=0,
                worker_id=worker_id,
                heartbeat=0,
            ))
        table = MockControlTable(control_table_item_list) # create a shared control table

        kinesis_ramp1.control_table = table
        kinesis_ramp2.control_table = table
        kinesis_ramp3.control_table = table

        def change_heartbeat(seconds):
            for j in xrange(1, 10):
                # change the first nines heartbeat to exclude them from rebalancing
                table.get_item(Key={'shard_id': 'shard-%s' % j})['Item']['heartbeat'] += 1

        with patch('time.sleep', change_heartbeat) as mock_method:
            self.assertTrue(kinesis_ramp1.can_claim_shard("shard-10"))
            self.assertTrue(kinesis_ramp1.claim_shard("shard-10"))

        def change_heartbeat10(seconds):
            for j in xrange(1, 11):
                # change the heartbeat for all shards so no workers seems idle
                table.get_item(Key={'shard_id': 'shard-%s' % j})['Item']['heartbeat'] += 1

        with patch('time.sleep', change_heartbeat10) as mock_method:
            # we should not be able to claim shard 10 since we have a optimal distribution of 3,3,4 and no workers are marked as idle
            self.assertFalse(kinesis_ramp2.can_claim_shard("shard-10"))

    def test_no_control_record(self):
        kinesis_ramp = self.get_kinesis_ramp()
        kinesis_ramp.control_table = MockControlTable([])
        kinesis_ramp.uncompleted_ids['shard-1'] = []
        self.assertRaises(NoItemsReturned, kinesis_ramp.can_claim_shard, 'shard_1')