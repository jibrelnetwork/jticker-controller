import sys
import logging.config

from aiohttp import web

from .handlers import (
    home,
)


logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'class': 'logging.Formatter',
            'format': '%(asctime)s %(levelname)-8s %(message)s',
        }
    },
    'handlers': {
        'console': {
            '()': 'logging.StreamHandler',
            'stream': sys.stdout,
            'formatter': 'default',
        },
    },
    'loggers': {
        'jticker_controller': {
            'level': 'DEBUG',
            'handlers': ['console'],
        },
    }
})


async def on_shutdown(app):
    pass


async def make_app(argv=None):
    """Create and initialize the application instance.
    """
    app = web.Application()
    app['config'] = {}

    app.on_shutdown.append(on_shutdown)

    # routes
    app.router.add_route('GET', '/', home)

    return app
