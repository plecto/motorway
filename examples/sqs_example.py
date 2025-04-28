import logging
import logging.config

from motorway.pipeline import Pipeline
from examples.ramps import WordRamp, ExampleSQSRamp
from examples.intersections import SentenceSplitIntersection, WordCountIntersection, ExampleSQSIntersection


logging.basicConfig(level=logging.DEBUG)

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
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word_to_sqs')
        self.add_intersection(ExampleSQSIntersection, 'word_to_sqs')

        self.add_ramp(ExampleSQSRamp, 'word_from_sqs')
        self.add_intersection(WordCountIntersection, 'word_from_sqs')


if __name__ == '__main__':
    WordCountPipeline().run()
