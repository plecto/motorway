from examples.intersections import SentenceSplitIntersection, AggregateIntersection, WordCountIntersection
from examples.ramps import WordRamp
from motorway.contrib.sql_alchemy.intersections import DatabaseInsertIntersection
from motorway.intersection import Intersection
from motorway.messages import Message
from motorway.pipeline import Pipeline
import sqlalchemy
import logging
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
        'level': 'WARN',
        'handlers': ['console']
    }

})



class SampleDatabaseInsertIntersection(DatabaseInsertIntersection):
    table = 'sample_tbl'
    database_uri = 'postgresql://localhost:5432/sampledb'

    table_columns = [
        # {'name': 'id', 'type': sqlalchemy.Integer, 'primary_key': True},
        {'name': 'word', 'type': sqlalchemy.String, 'primary_key': True},
        {'name': 'count', 'type': sqlalchemy.Integer},
    ]


class WordToDictIntersection(Intersection):
    def process(self, message):
        for word, count in message.content.items():
            yield Message.new(message, {
                'word': word,
                'count': count
            })
        self.ack(message)


class SamplePipeline(Pipeline):
    def definition(self):
        self.add_ramp(WordRamp, 'sentence')
        self.add_intersection(SentenceSplitIntersection, 'sentence', 'word')
        self.add_intersection(WordCountIntersection, 'word', 'formatter')
        self.add_intersection(WordToDictIntersection, 'formatter', 'insert_db')
        self.add_intersection(SampleDatabaseInsertIntersection, 'insert_db')


SamplePipeline().run()