import logging.config
import time


moment = time.strftime("%Y%m%d_%H%M%S", time.localtime())

LOGGING_CONFIG = {
    'version': 1,  # required
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(asctime)s %(levelname)s -- %(message)s'
        },
        'whenAndWhere': {
            'format': '%(asctime)s\t%(levelname)s -- %(processName)s %(filename)s:%(lineno)s -- %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'whenAndWhere'
        },
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console']
        },
    }
}

logging.config.dictConfig(LOGGING_CONFIG)
log = logging.getLogger('')  # factory method
log.setLevel(logging.CRITICAL)


def logit(iterator, message, indexes=None):

    for val in iterator:
        yield val
