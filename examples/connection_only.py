from motorway.pipeline import Pipeline
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



class ConnectionPipeline(Pipeline):
    def definition(self):
        pass


ConnectionPipeline(run_controller=False, run_webserver=False, run_connection_discovery=True).run()