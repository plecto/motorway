from collections import defaultdict
import random
import time
import uuid
from motorway.contrib.amazon_sqs.intersections import SQSInsertIntersection
from motorway.decorators import batch_process
from motorway.messages import Message
from motorway.intersection import Intersection


class SentenceSplitIntersection(Intersection):
    def process(self, message):
        for word in message.content.split(" "):
            yield Message(str(uuid.uuid4()), word, grouping_value=word)
        self.ack(message)


class WordCountIntersection(Intersection):
    def __init__(self, **kwargs):
        self._count = defaultdict(int)
        super(WordCountIntersection, self).__init__(**kwargs)

    @batch_process(wait=2, limit=5)
    def process(self, messages):
        for message in messages:
            # time.sleep(1)
            self._count[message.content] += 1
            self.ack(message)
        # time.sleep(0.5)
        yield Message(str(uuid.uuid4()), self._count)


class AggregateIntersection(Intersection):
    def __init__(self, **kwargs):
        self._count = defaultdict(int)
        super(AggregateIntersection, self).__init__(**kwargs)

    def process(self, message):
        self._count.update(message.content)
        yield Message(str(uuid.uuid4()), self._count)
        self.ack(message)


class AggregateConsumerIntersection(Intersection):
    def __init__(self, **kwargs):
        self._count = defaultdict(int)
        super(AggregateConsumerIntersection, self).__init__(**kwargs)

    def process(self, message):
        self._count.update(message.content)
        self.ack(message)
        print "Aggregate: ", self.process_uuid, self._count
        yield



class ExampleSQSIntersection(SQSInsertIntersection):
    queue_name = "tutorial_motorway"