import random
from Queue import Queue
import json
from threading import Thread, Lock, Semaphore
import time
import uuid
import datetime
import logging
import boto3
from motorway.messages import Message
from motorway.ramp import Ramp
from boto3.dynamodb.conditions import Attr
shard_election_logger = logging.getLogger("motorway.contrib.amazon_kinesis.shard_election")

logger = logging.getLogger(__name__)


class NoItemsReturned(Exception):
    pass


class KinesisRamp(Ramp):
    stream_name = None
    heartbeat_timeout = 30  # Wait 10 seconds for a heartbeat update, or kill it
    MAX_UNCOMPLETED_ITEMS = 3000
    GET_RECORDS_LIMIT = 1000

    def __init__(self, shard_threads_enabled=True, **kwargs):
        super(KinesisRamp, self).__init__(**kwargs)
        self.conn = boto3.client(**self.connection_parameters('kinesis'))
        assert self.stream_name, "Please define attribute stream_name on your KinesisRamp"

        control_table_name = self.get_control_table_name()

        self.worker_id = str(uuid.uuid4())
        self.semaphore = Semaphore()
        self.uncompleted_ids = {}
        self.dynamodb_client = boto3.client(**self.connection_parameters('dynamodb'))

        if shard_threads_enabled:
            self.dynamodb = boto3.resource(**self.connection_parameters('dynamodb'))

            try:
                self.dynamodb_client.describe_table(TableName=control_table_name)
            except self.dynamodb_client.exceptions.ResourceNotFoundException:
                self.dynamodb_client.create_table(
                    TableName=control_table_name,
                    KeySchema=[
                        {
                            'AttributeName': 'shard_id',
                            'KeyType': 'HASH'
                        },
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    },
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'shard_id',
                            'AttributeType': 'S'
                        },
                    ],
                )
                self.dynamodb_client.get_waiter('table_exists').wait(TableName=control_table_name)

            self.control_table = self.dynamodb.Table(control_table_name)
            shards = self.conn.describe_stream(StreamName=self.stream_name)['StreamDescription']['Shards']
            random.shuffle(shards)  # Start the threads in random order, in case of bulk restart
            threads = []
            self.insertion_queue = Queue()
            for i, shard in enumerate(shards):
                self.uncompleted_ids[shard['ShardId']] = set()
                t = Thread(target=self.process_shard, name="%s-%s" % (self.__class__.__name__, i), args=(shard['ShardId'], ))
                threads.append(t)
                t.start()

    def get_control_table_name(self):
        return 'pipeline-control-%s' % self.stream_name

    def claim_shard(self, shard_id):
        shard_election_logger.info("Claiming shard %s" % shard_id)
        try:
            control_record = self.control_table.get_item(Key={'shard_id': shard_id})['Item']
        except KeyError:
            raise NoItemsReturned()
        control_record['worker_id'] = self.worker_id
        control_record['heartbeat'] = 0
        try:
            self.control_table.put_item(Item=control_record,
                                        ConditionExpression=Attr('shard_id').eq(shard_id) & Attr('checkpoint').eq(control_record['checkpoint'])
                                        # ensure that the record was not changed between the get and put.
                                        )
        except self.dynamodb_client.exceptions.ConditionalCheckFailedException:  # Someone else edited the record
            shard_election_logger.debug("Failed to claim %s to %s" % (shard_id, self.worker_id))
            return False
        return True

    def can_claim_shard(self, shard_id):
        heartbeats = {}  # Store all heartbeats so we can compare them easily to track changes
        
        control_record = None
        shards = self.control_table.scan()['Items']
        for shard in shards:
            if shard['shard_id'] == shard_id:
                control_record = dict(shard)
            heartbeats[shard['worker_id']] = shard['heartbeat']

        if control_record is None:
            raise NoItemsReturned()

        heartbeats[self.worker_id] = 0
        time.sleep(self.heartbeat_timeout)
        updated_control_record = self.control_table.get_item(Key={'shard_id': shard_id})['Item']
        
        # Continue sleeping if heartbeat or worker id has changed
        if control_record['heartbeat'] == updated_control_record['heartbeat'] and control_record['worker_id'] == updated_control_record['worker_id']:
            # if both the heartbeat and the worker_id is the same
            shard_election_logger.debug("Shard %s - heartbeat and worker id remained unchanged for defined time, taking over" % shard_id)
            return True
        elif updated_control_record['worker_id'] != control_record['worker_id']:
            shard_election_logger.debug("Shard %s - Worker id changed to %s, continue sleeping" % (shard_id, updated_control_record['worker_id']))
        else:
            shard_election_logger.debug("Shard %s - Heartbeat changed, continue sleeping" % shard_id)
        
        # Balance, if possible
        active_workers = {
            self.worker_id: True
        }

        # re-fetch the shards and compare the heartbeat
        shards = self.control_table.scan()['Items']
        for shard in shards:  # Update active worker cache
            if heartbeats[shard['worker_id']] == shard['heartbeat']:
                active_workers[shard['worker_id']] = False
            else:
                active_workers[shard['worker_id']] = True

        number_of_active_workers = sum([1 for is_active in active_workers.values() if is_active])
        number_of_shards = len(shards)
        optimal_number_of_shards_per_worker = number_of_shards / number_of_active_workers
        workers = set([shard['worker_id'] for shard in shards])
        shards_per_worker = {worker: sum([1 for shard in shards if shard['worker_id'] == worker]) for worker in workers}
        for shard in shards:
            if shard['shard_id'] == shard_id:
                if (
                    # Check if the shards current worker has too many, or if the worker has no workers, then take
                    # the shard if the current worker has more than one shard!
                        shards_per_worker.get(shard['worker_id'], 0) > optimal_number_of_shards_per_worker or (
                                    shards_per_worker.get(self.worker_id, 0) == 0 and
                                    shards_per_worker.get(shard['worker_id'], 0) > 1
                                )
                ) and (
                    # Only get shards for balancing purposes, if we have too little
                        shards_per_worker.get(self.worker_id, 0) < optimal_number_of_shards_per_worker
                ):
                        shard_election_logger.debug("Taking over %s from %s" % (shard_id, shard['worker_id']))
                        return True
        return False

    def process_shard(self, shard_id):
        while True:
            try:
                # try to claim the shard
                try:
                    while True:
                        with self.semaphore:
                            if self.can_claim_shard(shard_id):
                                if self.claim_shard(shard_id):
                                    break
                        time.sleep(random.randrange(2, 15))
                except NoItemsReturned:
                    # no record for this shard found, nobody ever claimed the shard yet, so claim it
                    self.control_table.put_item(Item={
                        'shard_id': shard_id,
                        'checkpoint': 0,
                        'worker_id': self.worker_id,
                        'heartbeat': 0,
                    })

                # get initial iterator
                control_record = self.control_table.get_item(Key={'shard_id': shard_id})['Item']
                if control_record['checkpoint'] > 0:
                    # if we have a checkpoint, start from the checkpoint
                    iterator = self.conn.get_shard_iterator(
                        StreamName=self.stream_name,
                        ShardId=shard_id,
                        ShardIteratorType="AT_SEQUENCE_NUMBER",
                        StartingSequenceNumber=str(control_record['checkpoint'])
                    )['ShardIterator']
                else:
                    # we have no checkpoint stored, start from the latest item in Kinesis
                    iterator = self.conn.get_shard_iterator(
                        StreamName=self.stream_name,
                        ShardId=shard_id,
                        ShardIteratorType="LATEST",
                    )['ShardIterator']

                cloudwatch = boto3.client(**self.connection_parameters('cloudwatch'))
                current_minute = lambda: datetime.datetime.now().minute
                minute = None
                latest_item = None
                while True:
                    control_record = self.control_table.get_item(Key={'shard_id': shard_id})['Item']  # always retrieve this at the top of the loop
                    current_checkpoint = control_record['checkpoint']
                    current_heartbeat = control_record['heartbeat']
                    # if the shard was claimed by another worker, break out of the loop
                    if not control_record['worker_id'] == self.worker_id:
                        shard_election_logger.info("Lost shard %s, going back to standby" % shard_id)
                        break

                    # update the heartbeat and the checkpoint
                    control_record['heartbeat'] += 1
                    if len(self.uncompleted_ids[shard_id]):
                        # Get the "youngest" uncompleted sequence number
                        control_record['checkpoint'] = min(self.uncompleted_ids[shard_id])
                    elif latest_item:
                        # or the latest item we yielded
                        control_record['checkpoint'] = latest_item

                    self.control_table.put_item(Item=control_record,
                                                ConditionExpression=Attr('shard_id').eq(shard_id) & Attr('checkpoint').eq(current_checkpoint) & Attr('worker_id').eq(self.worker_id) & Attr('heartbeat').eq(current_heartbeat)
                                                # Will fail if someone else modified it - ConditionalCheckFailedException
                                                )

                    if len(self.uncompleted_ids[shard_id]) < self.MAX_UNCOMPLETED_ITEMS:
                        # get records from Kinesis, using the previously created iterator
                        result = self.conn.get_records(ShardIterator=iterator, Limit=self.GET_RECORDS_LIMIT)

                        # insert the records into the queue, and use the provided iterator for the next loop
                        for record in result['Records']:
                            self.uncompleted_ids[shard_id].add(record['SequenceNumber'])
                            latest_item = record['SequenceNumber']
                            self.insertion_queue.put(record)
                        iterator = result['NextShardIterator']
                    else:
                        logger.debug("Pausing, too many uncompleted items (%s/%s)" % (len(self.uncompleted_ids[shard_id]), self.MAX_UNCOMPLETED_ITEMS))
                        # we have too many uncompleted items, so back off for a while
                        # however, the iterator needs to be updated, because it expires after a while
                        # use the latest record we added to the queue as the starting point
                        next_iterator_number = latest_item if latest_item else str(control_record['checkpoint'])
                        iterator = self.conn.get_shard_iterator(
                            StreamName=self.stream_name,
                            ShardId=shard_id,
                            ShardIteratorType="AT_SEQUENCE_NUMBER",
                            StartingSequenceNumber=next_iterator_number
                        )['ShardIterator']

                        # get just one item to update the MillisBehindLatest below
                        result = self.conn.get_records(ShardIterator=iterator, Limit=1)

                    # Push metrics to CloudWatch
                    delay = result['MillisBehindLatest']
                    if minute != current_minute():  # push once per minute to CloudWatch.
                        minute = current_minute()
                        cloudwatch.put_metric_data(Namespace='Motorway/Kinesis',
                                                   MetricData=[{'MetricName': 'MillisecondsBehind',
                                                                'Dimensions': [{
                                                                    'Name': 'Stream',
                                                                    'Value': self.stream_name
                                                                }, {
                                                                    'Name': 'Shard',
                                                                    'Value': shard_id
                                                                }],
                                                                'Value': delay,
                                                                'Unit': 'Milliseconds'
                                                                }])

                    # recommended pause between fetches from AWS
                    time.sleep(1)
            except self.dynamodb_client.exceptions.ConditionalCheckFailedException as e:
                logger.warning(e)
                pass  # we're no longer worker for this shard
            except (self.dynamodb_client.exceptions.ProvisionedThroughputExceededException, self.dynamodb_client.exceptions.ProvisionedThroughputExceededException,
                    self.conn.exceptions.LimitExceededException, self.conn.exceptions.ProvisionedThroughputExceededException):
                logger.warning(e)
                time.sleep(random.randrange(5, self.heartbeat_timeout/2))  # back off for a while

    def connection_parameters(self, service_name):
        return {
            'region_name': 'eu-west-1',
            'service_name': service_name,
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }

    def next(self):
        msg = self.insertion_queue.get()
        try:
            yield Message(msg['SequenceNumber'], json.loads(msg['Data']), grouping_value=msg['PartitionKey'])
        except ValueError as e:
            logger.exception(e)

    def success(self, _id):
        for uncompleted_ids in self.uncompleted_ids.values():
            if _id in uncompleted_ids:
                uncompleted_ids.remove(_id)
