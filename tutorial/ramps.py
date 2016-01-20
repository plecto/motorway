import time
import uuid
from motorway.contrib.amazon_kinesis.ramps import KinesisRamp
from motorway.contrib.amazon_kinesis.intersections import KinesisInsertIntersection
from motorway.contrib.amazon_sqs.ramps import SQSRamp
from motorway.messages import Message
from motorway.ramp import Ramp
import random


class WordRamp(Ramp):
    fields = ['word']

    sentences = [
        "Oak is strong and also gives shade.",
        "Cats and dogs each hate the other.",
        "The pipe began to rust while new.",
        "Open the crate but don't break the glass.",
        "Add the sum to the product of these three.",
        "Thieves who rob friends deserve jail.",
        "The ripe taste of cheese improves with age.",
        "Act on these orders with great speed.",
        "The hog crawled under the high fence.",
        "Move the vat over the hot fire.",
    ]

    def __init__(self):
        super(WordRamp, self).__init__()
        self.limit = 1000
        self.progress = 0

    def next(self):
        # yield Message(uuid.uuid4().int, self.sentences[random.randint(0, len(self.sentences) -1)])
        if self.progress <= self.limit:
            self.progress += 1
            yield Message(uuid.uuid4().int, self.sentences[random.randint(0, len(self.sentences) -1)])
        else:
            time.sleep(1)

    def success(self, _id):
        print "WordRamp %s was successful" % _id

    def failed(self, _id):
        print "WordRamp %s has failed" % _id


class ExampleSQSRamp(SQSRamp):
    queue_name = "tutorial_motorway"


class ExampleKinesisRamp(KinesisRamp):
    stream_name = "data-pipeline-test"


class ExampleKinesisIntersection(KinesisInsertIntersection):
    stream_name = "data-pipeline-test"
