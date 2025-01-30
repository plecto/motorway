import os
from botocore.exceptions import ClientError


class SQSMixin(object):
    def init_queue(self):
        assert self.queue_name, "Please define attribute queue_name on your SQSRamp"
        # Get queue from cache
        self.queue = self.get_queue_from_cache() or self.get_queue_from_aws()

    def get_queue_from_aws(self):
        try:
            print(f"[DEBUG] Attempting to retrieve queue: {self.queue_name}")
            queue = self.sqs.get_queue_by_name(QueueName=self.queue_name)
            print(f"[DEBUG] Successfully retrieved queue URL: {queue.url}")
            return queue
        except ClientError as client_error:
            error_code = client_error.response.get('Error', {}).get('Code', '')
            print(f"[ERROR] ClientError while getting queue '{self.queue_name}': {error_code}")

            # Handle non-existent queue error
            if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                print(f"[DEBUG] Queue '{self.queue_name}' does not exist. Attempting to create it...")
                try:
                    queue = self.sqs.create_queue(QueueName=self.queue_name)
                    print(f"[DEBUG] Successfully created queue: {queue.url}")
                    return queue
                except ClientError as creation_error:
                    print(f"[ERROR] Failed to create queue '{self.queue_name}': {str(creation_error)}")
                    raise creation_error
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
            #'region_name': 'eu-west-1',
            #'service_name': 'sqs'
            # Add this or use ENV VARS
            # 'aws_access_key_id': '',
            # 'aws_secret_access_key': ''
            'service_name': os.getenv('SERVICE_NAME','sqs'),
            'region_name': os.getenv('REGION_NAME','eu-west-1'),
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'endpoint_url': os.getenv('ENDPOINT_URL')
        }
