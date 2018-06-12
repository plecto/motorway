from motorway.grouping import HashRingGrouper

from motorway.pipeline import Pipeline
from examples.ramps import WordRamp, ExampleSQSRamp, ExampleKinesisIntersection, ExampleKinesisRamp
from examples.intersections import SentenceSplitIntersection, WordCountIntersection, AggregateIntersection
import logging.config

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'colored': {
                '()': 'colorlog.ColoredFormatter',
                'format': "%(log_color)s%(levelname)-8s%(reset)s %(name)-32s %(processName)-32s %(blue)s%(message)s"
        }
    },
    'loggers': {
        'motorway': {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False
        },
        'werkzeug': {
            'level': 'WARN',
            'handlers': ['console'],
            'propagate': False,
        },

    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'colored'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    }

})


class WordCountPipeline(Pipeline):
    def definition(self):
        self.add_ramp(WordRamp, 'sentence')
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word_to_kinesis')
        self.add_intersection(ExampleKinesisIntersection, 'word_to_kinesis')

        self.add_ramp(ExampleKinesisRamp, 'word')
        self.add_intersection(WordCountIntersection, 'word', 'word_count', grouper_cls=HashRingGrouper, processes=1)
        self.add_intersection(AggregateIntersection, 'word_count', grouper_cls=HashRingGrouper, processes=1)


WordCountPipeline().run()