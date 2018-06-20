import json
import boto3
from motorway.decorators import batch_process
from motorway.intersection import Intersection
from time import sleep
import logging
logger = logging.getLogger(__name__)


class KinesisInsertIntersection(Intersection):
    stream_name = None

    def __init__(self, **kwargs):
        super(KinesisInsertIntersection, self).__init__(**kwargs)
        self.conn = boto3.client(**self.connection_parameters())
        assert self.stream_name, "Please define attribute stream_name on your KinesisInsertIntersection"

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            'service_name': 'kinesis',
            # Add this or use ENV VARS
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
        If any other error than ProvisionedThroughputExceededException or InternalFailure is returned in the response
        we log it using loglevel error and dump the message for replayability instead of raising an exception that would drop the whole batch.
        So if you are going to use this intersection in production be sure to monitor and handle the messages with log level error!
        :param messages: 
        :return: 
        """
        records = []
        for message in messages:
            kinesis_record = {'PartitionKey': message.grouping_value, 'Data': json.dumps(message.content)}
            records.append(kinesis_record)

        while len(records) > 0:
            logger.debug('put %s records' % len(records))
            response = self.conn.put_records(Records=records, StreamName=self.stream_name)
            records = []
            for i, record in enumerate(response['Records']):
                if len(record.get('ErrorCode', '')) > 0:
                    if record['ErrorCode'] in ['ProvisionedThroughputExceededException', 'InternalFailure']:
                        # retry when throttled or an internal failure
                        logger.warning(record['ErrorCode'])
                        kinesis_record = {'PartitionKey': messages[i].grouping_value, 'Data': json.dumps(messages[i].content)}
                        records.append(kinesis_record)
                    else:
                        # fail on any other exception
                        logger.error(record['ErrorMessage'])
                        self.fail(messages[i])
                else:
                    # success
                    self.ack(messages[i])

            if len(records) > 0:
                # wait to not get throttled again
                logger.warning('%s records were throttled, sleeping and re-trying...' % len(records))
                sleep(1)

        yield
