import logging

from motorway.pipeline import Pipeline
from examples.ramps import WordRamp, ExampleSQSRamp
from examples.intersections import SentenceSplitIntersection, WordCountIntersection, ExampleSQSIntersection


logging.basicConfig(level=logging.INFO)


class WordCountPipeline(Pipeline):
    def definition(self):
        self.add_ramp(WordRamp, 'sentence')
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word_to_sqs')
        self.add_intersection(ExampleSQSIntersection, 'word_to_sqs')

        self.add_ramp(ExampleSQSRamp, 'word_from_sqs')
        self.add_intersection(WordCountIntersection, 'word_from_sqs')


WordCountPipeline().run()