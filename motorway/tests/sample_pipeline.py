from collections import defaultdict
from motorway.messages import Message
from motorway.pipeline import Pipeline
from motorway.ramp import Ramp
from motorway.intersection import Intersection


class WordRamp(Ramp):
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
        self.completed = False
        self.successes = []
        self.errors = []

    def next(self):
        if not self.completed:
            for _id, sentence in enumerate(self.sentences):
                yield Message(_id, sentence)
            self.completed = True

    def success(self, _id):
        self.successes.append(_id)

    def failed(self, _id):
        self.errors.append(_id)


class SentenceSplitIntersection(Intersection):
    def process(self, message):
        for word in message.content.split(" "):
            yield Message.new(message, word, grouping_value=word)
        self.ack(message)


class WordCountIntersection(Intersection):
    def __init__(self):
        super(WordCountIntersection, self).__init__()
        self._count = defaultdict(int)

    def process(self, messages):
        for message in messages:
            self._count[message.content] += 1
            self.ack(message)


class SampleWordCountPipeline(Pipeline):
    def definition(self):
        self.add_ramp(WordRamp, 'sentence')
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word', processes=1)
        self.add_intersection(WordCountIntersection, 'word',)