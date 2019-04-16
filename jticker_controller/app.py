import os

import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from aiohttp import web

from .handlers import (
    home,
)
from .logging import _configure_logging


SENTRY_DSN = os.getenv('SENTRY_DSN')

if SENTRY_DSN:
    with open('version.txt', 'r') as fp:
        sentry_sdk.init(SENTRY_DSN, release=fp.read().strip(),
                        integrations=[AioHttpIntegration()])


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
