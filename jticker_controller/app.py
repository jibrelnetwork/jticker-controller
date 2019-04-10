import os

from aiohttp import web

from .handlers import (
    home,
)
from .logging import _configure_logging


async def on_shutdown(app):
    pass


async def make_app(argv=None):
    """Create and initialize the application instance.
    """
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    _configure_logging(LOG_LEVEL)
    app = web.Application()
    app['config'] = {}

    app.on_shutdown.append(on_shutdown)

    # routes
    app.router.add_route('GET', '/', home)

    return app
