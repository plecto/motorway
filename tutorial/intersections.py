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
    def __init__(self):
        self._count = defaultdict(int)
        super(WordCountIntersection, self).__init__()

    @batch_process(wait=2, limit=500)
    def process(self, messages):
        for message in messages:
            time.sleep(1)
            self._count[message.content] += 1
            self.ack(message)
        yield Message(str(uuid.uuid4()), self._count)


class AggregateIntersection(Intersection):
    def __init__(self):
        self._count = defaultdict(int)
        super(AggregateIntersection, self).__init__()

    def process(self, message):
        self._count.update(message.content)
        yield
        self.ack(message)
        print self._count


class ExampleSQSIntersection(SQSInsertIntersection):
    queue_name = "tutorial_motorway"