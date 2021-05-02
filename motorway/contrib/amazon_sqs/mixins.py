from botocore.exceptions import ClientError


class SQSMixin(object):
    def init_queue(self):
        assert self.queue_name, "Please define attribute queue_name on your SQSRamp"
        # Get queue from cache
        self.queue = self.get_queue_from_cache() or self.get_queue_from_aws()

    def get_queue_from_aws(self):
        try:
            return self.sqs.get_queue_by_name(QueueName=self.queue_name)
        except ClientError as client_error:
            # The queue doesn't exist and should be created
            if client_error.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                return self.sqs.create_queue(QueueName=self.queue_name)
            else:
                raise client_error

    """
        Instead of calling the aws api to get the queue url needed to instantiate the Queue object,
        You have the option to implement this method on the intersections using this class as Baseclass
        The queue url is what should be cached, and the queue can then be created with self.sqs.Queue(queue_url)
    """
    def get_queue_from_cache(self):
        return None

    def connection_parameters(self):
        return {
            'region_name': 'eu-west-1',
            'service_name': 'sqs'
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
        }
