import asyncio
from argparse import ArgumentParser

import sentry_sdk
import mode
from loguru import logger
from addict import Dict

from jticker_core import configure_logging, ignore_aiohttp_ssl_eror, inject, register

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
    # influx
    parser.add_argument("--influx-host", default="influxdb",
                        help="Influxdb hosts (comma separated) [default: %(default)s]")
    parser.add_argument("--influx-port", default="8086",
                        help="Influxdb port [default: %(default)s]")
    parser.add_argument("--influx-db", default="test",
                        help="Influxdb db [default: %(default)s]")
    parser.add_argument("--influx-username", default=None, help="Influxdb username [%(default)s]")
    parser.add_argument("--influx-password", default=None, help="Influxdb password [%(default)s]")
    parser.add_argument("--influx-ssl", action="store_true", default=False,
                        help="Influxdb use ssl [%(default)s]")
    parser.add_argument("--influx-unix-socket", default=None,
                        help="Influxdb unix socket [%(default)s]")
    parser.add_argument("--influx-chunk-size", default="1000",
                        help="Influx batch/chunk write size [%(default)s]")
    return parser


@register(singleton=True)
@inject
def host(config: Dict) -> str:
    return config.web_host


@register(singleton=True)
@inject
def port(config: Dict) -> str:
    return config.web_port


class Worker(mode.Worker):
    """Patched `Worker` with disabled `_setup_logging` and graceful shutdown.
    Default `mode.Worker` overrides logging configuration. This hack is there to
    deny this behavior.
    """

    def _setup_logging(self) -> None:
        pass


@inject
def main(config: Dict, version: str, controller: Controller):
    loop = asyncio.get_event_loop()
    ignore_aiohttp_ssl_eror(loop, "3.5.4")
    configure_logging(config.log_level)
    if config.sentry_dsn:
        sentry_sdk.init(config.sentry_dsn, release=version)
    logger.info("Jticker controller version {}", version)
    Worker(controller).execute_from_commandline()


main()
