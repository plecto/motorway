"""
You need https://pypi.python.org/pypi/simple-salesforce
"""
import os

import requests
from simple_salesforce import SFType
from simple_salesforce import Salesforce

from motorway.intersection import Intersection


class SalesforceInsertIntersection(Intersection):
    upsert = True
    object_api_name = None
    upsert_id_field = None

    SF_USERNAME = os.environ.get('SF_USERNAME')
    SF_PASSWORD = os.environ.get('SF_PASSWORD')
    SF_SECURITY_TOKEN = os.environ.get('SF_SECURITY_TOKEN')

    def __init__(self, *args, **kwargs):
        super(SalesforceInsertIntersection, self).__init__(*args, **kwargs)
        session = requests.Session()
        self.sf_instance = Salesforce(
            username=self.SF_USERNAME,
            password=self.SF_PASSWORD,
            security_token=self.SF_SECURITY_TOKEN,
            session=session
        )
        self.sf_type = SFType(self.object_api_name, self.sf_instance.session_id, self.sf_instance.sf_instance, self.sf_instance.sf_version, self.sf_instance.proxies)

    def process(self, message):
        if self.upsert:
            self.sf_type.upsert(
                '%s/%s' % (self.upsert_id_field, message.content.pop(self.upsert_id_field)),
                message.content
            )
        else:
            self.sf_type.create(message.content)
        self.ack(message)