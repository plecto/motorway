import logging
from motorway.grouping import HashRingGrouper, SendToAllGrouper

from motorway.pipeline import Pipeline
from examples.ramps import WordRamp
from examples.intersections import SentenceSplitIntersection, WordCountIntersection, AggregateIntersection, \
    AggregateConsumerIntersection

# logging.basicConfig(level=logging.INFO, format=)

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
            'level': 'WARN',
            'handlers': ['console'],
            'propagate': False
        },
        'werkzeug': {
            'level': 'WARN',
            'handlers': ['console'],
            'propagate': False,
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'colored'
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console']
    }

})


class WordCountPipeline(Pipeline):
    def definition(self):
        self.add_ramp(WordRamp, 'sentence')
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word', processes=2)
        self.add_intersection(WordCountIntersection, 'word', 'word_count', grouper_cls=HashRingGrouper, processes=2)
        self.add_intersection(AggregateIntersection, 'word_count', 'aggregate', processes=1)
        self.add_intersection(AggregateConsumerIntersection, 'aggregate', processes=2, grouper_cls=SendToAllGrouper)


WordCountPipeline().run()