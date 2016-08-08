import json
import time
import os
import requests

from motorway.messages import Message
from motorway.ramp import Ramp
from simple_salesforce import Salesforce
from simple_salesforce.api import SFType



class SalesforceStreamingObjectRamp(Ramp):
    topic = 'YourTopic'


    def __init__(self, *args, **kwargs):
        super(SalesforceStreamingObjectRamp, self).__init__(*args, **kwargs)
        self.sf = Salesforce(username=self.sf_username, password=self.sf_password, security_token=self.sf_token)
        self.session = requests.Session()  # Important, it needs cookies to be persistent

        connect = self.session.post("https://%s/cometd/37.0" % self.sf.sf_instance, headers=self.sf.headers,
                                    data=json.dumps({
                                        "version": "1.0",
                                        "minimumVersion": "0.9",
                                        "channel": "/meta/handshake",
                                        "supportedConnectionTypes": ["long-polling"],
                                        "advice": {
                                             "timeout": 2500,
                                             "interval": 0
                                        },
                                        "id": "1"
                                    }))

        self.connection = connect.json()[0]

        subscribe = self.session.post("https://%s/cometd/37.0" % self.sf.sf_instance, headers=self.sf.headers,
                                      data=json.dumps({
                                          "channel": "/meta/subscribe",
                                          "subscription": "/topic/%s" % self.topic,
                                          "clientId": self.connection['clientId']
                                      }))
        assert subscribe.json()[0]['successful']

    @property
    def sf_username(self):
        return os.environ.get("SALESFORCE_USERNAME")

    @property
    def sf_password(self):
        return os.environ.get("SALESFORCE_PASSWORD")

    @property
    def sf_token(self):
        return os.environ.get("SALESFORCE_TOKEN")

    def next(self):
        updates = self.session.post("https://%s/cometd/37.0" % self.sf.sf_instance, headers=self.sf.headers,
                           data=json.dumps({
                               "channel": "/meta/connect",
                               "connectionType": "long-polling",
                               "clientId": self.connection['clientId']
                           }))
        for update in updates.json():
            if 'data' in update:
                if 'sobject' in update['data']:
                    yield Message(update['data']['sobject']['Id'],
                        update['data']['sobject']
                    )


class SalesforceQueryObjectRamp(Ramp):
    query = 'SELECT Id, Amount FROM Opportunity LIMIT 10'
    sleep_time = 60 * 30  # Every 30 min


    def __init__(self, *args, **kwargs):
        super(SalesforceQueryObjectRamp, self).__init__(*args, **kwargs)
        self.sf = Salesforce(username=self.sf_username, password=self.sf_password, security_token=self.sf_token)

    @property
    def sf_username(self):
        return os.environ.get("SALESFORCE_USERNAME")

    @property
    def sf_password(self):
        return os.environ.get("SALESFORCE_PASSWORD")

    @property
    def sf_token(self):
        return os.environ.get("SALESFORCE_TOKEN")

    def next(self):
        for obj in self.sf.query_all(self.query)['records']:
            attributes = obj.pop('attributes')
            yield Message(
                obj['Id'],
                obj
            )
        time.sleep(self.sleep_time)
