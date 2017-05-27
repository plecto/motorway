import random
from Queue import Queue
import json
from threading import Thread, Lock, Semaphore
import time
import uuid

import datetime
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
import boto.ec2.cloudwatch
import boto.kinesis.exceptions
import logging


shard_election_logger = logging.getLogger("motorway.contrib.amazon_kinesis.shard_election")


class KinesisRamp(Ramp):
    stream_name = None
    heartbeat_timeout = 30  # Wait 10 seconds for a heartbeat update, or kill it
    MAX_UNCOMPLETED_ITEMS = 3000
    GET_RECORDS_LIMIT = 1000

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
            shards = random.shuffle(shards)  # Start the threads in random order, in case of bulk restart
            threads = []
            self.insertion_queue = Queue()
            for i, shard in enumerate(shards):
                self.uncompleted_ids[shard['ShardId']] = set()
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
            shard_election_logger.debug("Shard %s - heartbeat remained unchanged for defined time, taking over" % shard_id)
            return True
        else:
            shard_election_logger.debug("Shard %s - Heartbeat changed, continue sleeping" % shard_id)
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
                except ItemNotFound:
                    # no record for this shard found, nobody ever claimed the shard yet, so claim it
                    control_record = Item(self.control_table, data={
                        'shard_id': shard_id,
                        'checkpoint': 0,
                        'worker_id': self.worker_id,
                        'heartbeat': 0,
                    })
                    control_record.save()

                # get initial iterator
                control_record = self.control_table.get_item(shard_id=shard_id)
                if control_record['checkpoint'] > 0:
                    # if we have a checkpoint, start from the checkpoint
                    iterator = self.conn.get_shard_iterator(
                        self.stream_name,
                        shard_id,
                        "AT_SEQUENCE_NUMBER",
                        starting_sequence_number=str(control_record['checkpoint'])
                    )['ShardIterator']
                else:
                    # we have no checkpoint stored, start from the latest item in Kinesis
                    iterator = self.conn.get_shard_iterator(
                        self.stream_name,
                        shard_id,
                        "LATEST",
                    )['ShardIterator']

                cloudwatch = boto.ec2.cloudwatch.connect_to_region(**self.connection_parameters())
                current_minute = lambda: datetime.datetime.now().minute
                minute = None
                latest_item = None
                while True:
                    control_record = self.control_table.get_item(shard_id=shard_id)  # always retrieve this at the top of the loop

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
                    control_record.save()  # Will fail if someone else modified it - ConditionalCheckFailedException

                    # get records from Kinesis, using the previously created iterator
                    result = self.conn.get_records(iterator, limit=self.GET_RECORDS_LIMIT)

                    if len(self.uncompleted_ids) < self.MAX_UNCOMPLETED_ITEMS:
                        # insert the records into the queue, and use the provided iterator for the next loop
                        for record in result['Records']:
                            self.uncompleted_ids[shard_id].add(record['SequenceNumber'])
                            latest_item = record['SequenceNumber']
                            self.insertion_queue.put(record)
                        iterator = result['NextShardIterator']
                    else:
                        # we have too many uncompleted items, so back off for a while
                        # however, the iterator needs to be updated, because it expires after a while
                        # use the latest record we added to the queue as the starting point
                        next_iterator_number = latest_item if latest_item else str(control_record['checkpoint'])
                        iterator = self.conn.get_shard_iterator(
                            self.stream_name,
                            shard_id,
                            "AT_SEQUENCE_NUMBER",
                            starting_sequence_number=next_iterator_number
                        )['ShardIterator']

                    # Push metrics to CloudWatch
                    delay = result['MillisBehindLatest']
                    if minute != current_minute():  # push once per minute to CloudWatch.
                        minute = current_minute()
                        cloudwatch.put_metric_data(
                            'Motorway/Kinesis',
                            'MillisecondsBehind',
                            value=delay,
                            unit='Milliseconds',
                            dimensions={
                                'Stream': self.stream_name,
                                'Shard': shard_id
                            }
                        )

                    # recommended pause between fetches from AWS
                    time.sleep(1)
            except ConditionalCheckFailedException:
                pass  # we're no longer worker for this shard
            except (ProvisionedThroughputExceededException, LimitExceededException, boto.kinesis.exceptions.ProvisionedThroughputExceededException, boto.kinesis.exceptions.LimitExceededException) as e:
                time.sleep(random.randrange(5, self.heartbeat_timeout/2))  # back off for a while

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
        for uncompleted_ids in self.uncompleted_ids.values():
            if _id in uncompleted_ids:
                uncompleted_ids.remove(_id)
