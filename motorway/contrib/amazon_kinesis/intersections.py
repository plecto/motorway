import json
import boto.kinesis
from motorway.decorators import batch_process
from motorway.intersection import Intersection
from time import sleep


class KinesisInsertIntersection(Intersection):
    stream_name = None

    def __init__(self, **kwargs):
        super(KinesisInsertIntersection, self).__init__(**kwargs)
        self.conn = boto.kinesis.connect_to_region(**self.connection_parameters())
        assert self.stream_name, "Please define attribute stream_name on your KinesisInsertIntersection"

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            # Add this or use ENV VARS
            #'aws_access_key_id': '',
            #'aws_secret_access_key': ''
        }

    @batch_process(limit=500, wait=1)
    def process(self, messages):
        """
        wait 1 second and get up to 500 items 
        Each PutRecords request can support up to 500 records. 
        Each record in the request can be as large as 1 MB, up to a limit of 5 MB for the entire request, including partition keys. 
        Each shard can support writes up to 1,000 records per second, up to a maximum data write total of 1 MB per second.
        This means we can run 2 intersections (2 x 500 records) submitting to the same shard before hitting the write limit (1000 records/sec)
        If we hit the write limit we wait 2 seconds and try to send the records that failed again, rinse and repeat
        :param messages: 
        :return: 
        """
        records = []
        for message in messages:
            kinesis_record = {'PartitionKey': message.grouping_value, 'Data': json.dumps(message.content)}
            records.append(kinesis_record)
        response = self.conn.put_records(records, self.stream_name)
        failed_records = []
        for i, record in enumerate(response['Records']):
            if len(record.get('ErrorCode', '')) > 0:
                failed_records.append(messages[i])
            else:
                self.ack(messages[i])

        if len(failed_records) > 0:
            sleep(2)
            self.process(failed_records)

        yield
