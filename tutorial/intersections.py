from collections import defaultdict
import random
import time
from motorway.contrib.amzon_sqs.intersections import SQSInsertIntersection
from motorway.decorators import batch_process
from motorway.messages import Message
from motorway.intersection import Intersection


class SentenceSplitIntersection(Intersection):
    def process(self, message):
        for word in message.content.split(" "):
            yield Message.new(message, word, grouping_value=word)
        message.ack()


class WordCountIntersection(Intersection):
    def __init__(self):
        self._count = defaultdict(int)
        super(WordCountIntersection, self).__init__()

    @batch_process(wait=2, limit=500)
    def process(self, messages):
        for message in messages:
            self._count[message.content] += 1
            message.ack()
        yield Message(0, self._count)


class AggregateIntersection(Intersection):
    def __init__(self):
        self._count = defaultdict(int)
        super(AggregateIntersection, self).__init__()

    def process(self, message):
        self._count.update(message.content)
        yield
        message.ack()
        print self._count


class ExampleSQSIntersection(SQSInsertIntersection):
    queue_name = "tutorial_motorway"