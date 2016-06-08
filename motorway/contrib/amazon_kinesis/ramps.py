import random
from Queue import Queue
import json
from threading import Thread, Lock, Semaphore
import time
import uuid
from boto.dynamodb2.exceptions import ItemNotFound, ConditionalCheckFailedException, \
    ProvisionedThroughputExceededException, LimitExceededException
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import STRING
from boto.exception import JSONResponseError
from motorway.messages import Message
from motorway.ramp import Ramp
import boto.kinesis
import boto.kinesis.exceptions
import logging


shard_election_logger = logging.getLogger("motorway.contrib.amazon_kinesis.shard_election")


class KinesisRamp(Ramp):
    stream_name = None
    heartbeat_timeout = 30  # Wait 10 seconds for a heartbeat update, or kill it

    def __init__(self, shard_threads_enabled=True, **kwargs):
        super(KinesisRamp, self).__init__(**kwargs)
        self.conn = boto.kinesis.connect_to_region(**self.connection_parameters())
        assert self.stream_name, "Please define attribute stream_name on your KinesisRamp"

        control_table_name = 'pipeline-control-%s' % self.stream_name

        self.worker_id = str(uuid.uuid4())
        self.semaphore = Semaphore()
        self.uncompleted_ids = {}

        if shard_threads_enabled:
            dynamodb_connection = boto.dynamodb2.connect_to_region(**self.connection_parameters())

            table_config = dict(
                connection=dynamodb_connection,
                schema=[
                    HashKey('shard_id', data_type=STRING),
                ],
                throughput={
                    'read': 10,
                    'write': 10,
                }
            )

            try:
                dynamodb_connection.describe_table(control_table_name)
            except JSONResponseError:
                Table.create(
                    control_table_name,
                    **table_config
                )

            self.control_table = Table(
                control_table_name,
                **table_config
            )

            shards = self.conn.describe_stream(self.stream_name)['StreamDescription']['Shards']
            threads = []
            self.insertion_queue = Queue()
            for i, shard in enumerate(shards):
                self.uncompleted_ids[shard['ShardId']] = []
                t = Thread(target=self.process_shard, name="%s-%s" % (self.__class__.__name__, i), args=(shard['ShardId'], ))
                threads.append(t)
                t.start()

    def claim_shard(self, shard_id):
        shard_election_logger.info("Claiming shard %s" % shard_id)
        control_record = self.control_table.get_item(shard_id=shard_id)
        control_record['worker_id'] = self.worker_id
        control_record['heartbeat'] = 0
        try:
            control_record.save()
        except ConditionalCheckFailedException:  # Someone else edited the record
            shard_election_logger.debug("Failed to claim %s to %s" % (shard_id, self.worker_id))
            return False
        return True

    def can_claim_shard(self, shard_id):
        control_record = self.control_table.get_item(shard_id=shard_id)
        heartbeat = control_record['heartbeat']
        time.sleep(self.heartbeat_timeout)
        if heartbeat == self.control_table.get_item(shard_id=shard_id)['heartbeat']:  # TODO: check worker_id as well
            shard_election_logger.debug("Heartbeat remained unchanged for defined time, taking over")
            return True
        else:
            shard_election_logger.debug("Heartbeat changed, continue sleeping")
        # Balance, if possible
        active_workers = {
            self.worker_id: True
        }
        heartbeats = {
            self.worker_id: 0
        }

        shards = list(self.control_table.scan())
        for shard in shards:  # Update active worker cache
            if shard['worker_id'] not in active_workers:
                heartbeats[shard['worker_id']] = shard['heartbeat']
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
            if shards_per_worker.get(shard['worker_id'], 0) > optimal_number_of_shards_per_worker:
                if shards_per_worker.get(self.worker_id, 0) < optimal_number_of_shards_per_worker:
                    if shard['shard_id'] == shard_id:
                        shard_election_logger.debug("Taking over %s from %s" % (shard_id, shard['worker_id']))
                        return True
        return False

    def process_shard(self, shard_id):
        while True:
            try:
                try:
                    while True:
                        with self.semaphore:
                            if self.can_claim_shard(shard_id):
                                if self.claim_shard(shard_id):
                                    break
                        time.sleep(random.randrange(2, 15))
                except ItemNotFound:
                    control_record = Item(self.control_table, data={
                        'shard_id': shard_id,
                        'checkpoint': 0,
                        'worker_id': self.worker_id,
                        'heartbeat': 0,
                    })
                    control_record.save()


                control_record = self.control_table.get_item(shard_id=shard_id)
                if control_record['checkpoint'] > 0:
                    iterator = self.conn.get_shard_iterator(
                        self.stream_name,
                        shard_id,
                        "AT_SEQUENCE_NUMBER",
                        starting_sequence_number=str(control_record['checkpoint'])
                    )['ShardIterator']
                else:
                    iterator = self.conn.get_shard_iterator(
                        self.stream_name,
                        shard_id,
                        "LATEST",
                    )['ShardIterator']
                while True:
                    control_record = self.control_table.get_item(shard_id=shard_id)
                    if not control_record['worker_id'] == self.worker_id:
                        shard_election_logger.info("Lost shard %s, going back to standby" % shard_id)
                        break
                    control_record['heartbeat'] += 1
                    if len(self.uncompleted_ids[shard_id]):  # Get the "youngest" uncompleted sequence number
                        control_record['checkpoint'] = min(self.uncompleted_ids[shard_id])
                    control_record.save()

                    result = self.conn.get_records(iterator)
                    for record in result['Records']:
                        self.uncompleted_ids[shard_id].append(record['SequenceNumber'])
                        self.insertion_queue.put(record)
                    iterator = result['NextShardIterator']
                    time.sleep(1)
            except ConditionalCheckFailedException:
                pass  # we're no longer worker for this shard
            except (ProvisionedThroughputExceededException, LimitExceededException, boto.kinesis.exceptions.ProvisionedThroughputExceededException, boto.kinesis.exceptions.LimitExceededException) as e:
                time.sleep(random.randrange(5, self.heartbeat_timeout))  # back off for a while


    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }

    def next(self):
        msg = self.insertion_queue.get()
        yield Message(msg['SequenceNumber'], json.loads(msg['Data']), grouping_value=msg['PartitionKey'])

    def success(self, _id):
        for shard_id, uncompleted_ids in self.uncompleted_ids.items():
            if _id in uncompleted_ids:
                uncompleted_ids.remove(_id)