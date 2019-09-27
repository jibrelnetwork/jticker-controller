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
from aioinflux import InfluxDBClient

from jticker_core import inject, register, WebServer, Task
from jticker_core.candle import EPOCH_START


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
        )
        self.influx_clients = [cli(host=h) for h in config.influx_host.split(",")]
        self._task_topic = config.kafka_tasks_topic
        self._version = version
        self._configure_router()

    def _configure_router(self):
        self.web_server.app.router.add_route("POST", "/task/add", self._add_task)
        self.web_server.app.router.add_route("GET", "/storage/strip", self._strip)
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

    async def _strip(self, request):
        time_iso8601 = datetime.datetime.fromtimestamp(EPOCH_START).isoformat()
        coros = [c.query(f"delete where time < '{time_iso8601}'") for c in self.influx_clients]
        await asyncio.gather(*coros)
        raise web.HTTPOk()

    async def _healthcheck(self, request):
        return web.json_response(dict(healthy=True, version=self._version))
