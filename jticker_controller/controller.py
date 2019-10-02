import asyncio
import uuid
import datetime
from functools import partial

import backoff
from aiohttp import web
from mode import Service
from aiokafka import AIOKafkaProducer
from aiokafka.errors import ConnectionError as KafkaConnectionError
from addict import Dict
from aioinflux import InfluxDBClient, iterpoints
from loguru import logger
from tqdm import tqdm

from jticker_core import inject, register, WebServer, Task
from jticker_core.candle import EPOCH_START


class LogFile:

    def __init__(self, logger):
        self.logger = logger

    def write(self, s: str):
        s = s.strip()
        if not s:
            return
        self.logger.info(s)

    def flush(self):
        pass


@register(singleton=True, name="controller")
class Controller(Service):

    @inject
    def __init__(self, web_server: WebServer, config: Dict, version: str):
        super().__init__()
        self.web_server = web_server
        self._producer = AIOKafkaProducer(
            loop=asyncio.get_event_loop(),
            bootstrap_servers=config.kafka_bootstrap_servers.split(","),
            key_serializer=lambda s: s.encode("utf-8"),
            value_serializer=lambda s: s.encode("utf-8"),
        )
        cli = partial(
            InfluxDBClient,
            port=int(config.influx_port),
            db=config.influx_db,
            unix_socket=config.influx_unix_socket,
            ssl=bool(config.influx_ssl),
            username=config.influx_username,
            password=config.influx_password,
            timeout=60 * 60,
        )
        self.influx_clients = [cli(host=h) for h in config.influx_host.split(",")]
        self._task_topic = config.kafka_tasks_topic
        self._version = version
        self._configure_router()

    def _configure_router(self):
        self.web_server.app.router.add_route("POST", "/task/add", self._add_task)
        self.web_server.app.router.add_route("GET", "/storage/strip", self._schedule_strip)
        self.web_server.app.router.add_route("GET", "/healthcheck", self._healthcheck)

    def on_init_dependencies(self):
        return [
            self.web_server,
        ]

    @backoff.on_exception(
        backoff.expo,
        (KafkaConnectionError,),
        max_time=120)
    async def on_start(self):
        await self._producer.start()

    async def on_stop(self):
        await self._producer.stop()
        await asyncio.gather(*[c.close() for c in self.influx_clients])

    async def _add_task(self, request):
        data = await request.json()
        task = Task.from_dict(data)
        await self._producer.send_and_wait(
            self._task_topic,
            value=task.as_json(),
            key=uuid.uuid4().hex,
        )
        raise web.HTTPOk()

    async def _strip(self):
        # TODO: simplify when resolved
        # https://github.com/influxdata/influxdb/issues/9636
        logger.info("strip started")
        coros = [c.query("show measurements") for c in self.influx_clients]
        results = await asyncio.gather(*coros)
        topics = set()
        for points in results:
            for p in iterpoints(points):
                topics.add(p[0])
        total = len(topics)
        logger.info("will strip {} topics", total)
        time_iso8601 = datetime.datetime.fromtimestamp(EPOCH_START).date().isoformat()
        for topic in tqdm(topics, ncols=80, ascii=True, mininterval=10,
                          maxinterval=10, file=LogFile(logger=logger)):
            coros = [c.query(f"""delete from "{topic}" where time < '{time_iso8601}'""")
                     for c in self.influx_clients]
            await asyncio.gather(*coros)
        logger.info("strip done")

    async def _schedule_strip(self, request):
        logger.info("strip scheduled")
        self.add_future(self._strip())
        raise web.HTTPOk()

    async def _healthcheck(self, request):
        return web.json_response(dict(healthy=True, version=self._version))
