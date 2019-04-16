import logging.config

ALLOWED_LOG_LEVELS = {'ERROR', 'INFO', 'DEBUG', 'WARN'}


def _configure_logging(log_level='INFO'):
    assert log_level in ALLOWED_LOG_LEVELS, "Invalid LOG_LEVEL %s" % log_level

    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'default': {
                'class': 'pythonjsonlogger.jsonlogger.JsonFormatter',
                'format': '%(asctime)s %(levelname)-8s %(message)s'
            }
        },
        'handlers': {
            'console': {
                '()': 'logging.StreamHandler',
                'formatter': 'default'
            },
        },
        'loggers': {
            'jticker_controller': {
                'level': log_level,
                'handlers': ['console'],
            },
        }
    })
