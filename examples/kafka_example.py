import logging.config

from motorway.pipeline import Pipeline
from examples.ramps import WordRamp, ExampleKafkaRamp, SecondExampleKafkaRamp
from examples.intersections import SentenceSplitIntersection, WordCountIntersection, ExampleKafkaIntersection


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
        'motorway.contrib.kafka': {
            'level': 'DEBUG',
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
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word_to_kafka')
        self.add_intersection(ExampleKafkaIntersection, 'word_to_kafka')

        self.add_ramp(ExampleKafkaRamp, 'word_from_kafka')
        # add second kafka ramp -- it will work initially but will stop after 10 iterations
        self.add_ramp(SecondExampleKafkaRamp, 'word_from_kafka')
        self.add_intersection(WordCountIntersection, 'word_from_kafka')


if __name__ == '__main__':
    WordCountPipeline().run()
