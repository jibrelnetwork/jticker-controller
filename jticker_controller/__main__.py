import asyncio
from argparse import ArgumentParser

import sentry_sdk
from loguru import logger
from addict import Dict

from jticker_core import configure_logging, ignore_aiohttp_ssl_eror, inject, register, Worker

from .controller import Controller


@register(singleton=True)
def name():
    return "jticker-controller"


@register(singleton=True)
@inject
def parser(name, base_parser):
    parser = ArgumentParser(name, parents=[base_parser])
    parser.add_argument("--stats-log-interval", default="60",
                        help="Stats logging interval [default: %(default)s]")
    parser.add_argument("--add-candles-batch-size", default="1000",
                        help="Add candles batch size [default: %(default)s]")
    parser.add_argument("--add-candles-batch-timeout", default="600",
                        help="Add candles batch timeout [default: %(default)s]")
    # kafka
    parser.add_argument("--kafka-bootstrap-servers", default="kafka:9092",
                        help="Comma separated kafka bootstrap servers [default: %(default)s]")
    parser.add_argument("--kafka-tasks-topic", default="grabber_tasks",
                        help="Tasks kafka topic [default: %(default)s]")
    parser.add_argument("--kafka-trading-pairs-topic", default="assets_metadata",
                        help="Trading pairs kafka topic [default: %(default)s]")
    return parser


@register(singleton=True)
@inject
def host(config: Dict) -> str:
    return config.web_host


@register(singleton=True)
@inject
def port(config: Dict) -> str:
    return config.web_port


@inject
def main(config: Dict, version: str, controller: Controller):
    loop = asyncio.get_event_loop()
    ignore_aiohttp_ssl_eror(loop, "3.5.4")
    configure_logging()
    if config.sentry_dsn:
        sentry_sdk.init(config.sentry_dsn, release=version)
    logger.info("Jticker controller version {}", version)
    Worker(controller).execute_from_commandline()


main()
