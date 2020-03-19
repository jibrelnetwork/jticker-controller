import asyncio
from functools import wraps
from importlib import resources
from typing import Dict

from addict import Dict as AdDict
from aiohttp import ClientSession, web
from jibrel_aiohttp_swagger import setup_swagger
from loguru import logger
from mode import Service

from jticker_core import AbstractTimeSeriesStorage, Interval, WebServer, inject, register
from jticker_core.time_series_storage.influx import InfluxTimeSeriesStorage


class AsyncResourceContext:

    def __init__(self, resource, *, enter_name="start", exit_name="stop"):
        self.resource = resource
        self.enter_name = enter_name
        self.exit_name = exit_name

    async def __aenter__(self):
        if self.enter_name:
            await getattr(self.resource, self.enter_name)()
        return self.resource

    async def __aexit__(self, *exc_info):
        if self.exit_name:
            return await getattr(self.resource, self.exit_name)()


@register(singleton=True, name="controller")
class Controller(Service):

    ALLOWED_CANDLE_INTERVALS = {
        Interval.MIN_1.value,
        Interval.D_1.value,
    }

    @inject
    def __init__(self, web_server: WebServer, config: AdDict, version: str):
        super().__init__()
        self.web_server = web_server
        self.config = config
        self._bootstrap_servers = config.kafka_bootstrap_servers.split(",")
        self._time_series_by_host: Dict[str, AbstractTimeSeriesStorage] = {}
        self._batches_queue: asyncio.Queue = asyncio.Queue(maxsize=10)
        self._task_topic = config.kafka_tasks_topic
        self._assets_metadata_topic = config.kafka_trading_pairs_topic
        self._add_candles_batch_size = int(config.add_candles_batch_size)
        self._add_candles_batch_timeout = int(config.add_candles_batch_timeout)
        self._version = version
        self._configure_router()

    def _configure_router(self):
        self.web_server.app.router.add_route("GET", "/storage/{host}/migrate", self._migrate)
        self.web_server.app.router.add_route("GET", "/storage/{host}/query", self._storage_query)
        self.web_server.app.router.add_route("*", "/storage/{host}/raw/query",
                                             self._storage_raw_query)
        self.web_server.app.router.add_route("GET", "/healthcheck", self._healthcheck)
        with resources.path("jticker_controller", "api-spec.yml") as path:
            setup_swagger(self.web_server.app, str(path))

    def on_init_dependencies(self):
        return [
            self.web_server,
        ]

    async def on_started(self):
        self._raw_storage_session = ClientSession()

    async def on_stop(self):
        await self._raw_storage_session.close()

    def background_task_handler(f):
        @wraps(f)
        async def wrapper(self, request):
            logger.info("scheduling {}", f.__name__)
            self.add_future(f(self, request))
            raise web.HTTPOk()
        return wrapper

    async def get_time_series(self, host: str) -> AbstractTimeSeriesStorage:
        if host not in self._time_series_by_host:
            config = self.config.copy()
            config.time_series_host = host
            ts = self._time_series_by_host[host] = InfluxTimeSeriesStorage(config)
            await self.add_runtime_dependency(ts)
        return self._time_series_by_host[host]

    async def resolve_time_series(self, request: web.Request) -> AbstractTimeSeriesStorage:
        host = request.match_info["host"]
        return await self.get_time_series(host)

    @background_task_handler  # type: ignore
    async def _migrate(self, request):
        ts = await self.resolve_time_series(request)
        await ts.migrate()

    async def _storage_query(self, request):
        query = request.query.get("query")
        if query is None:
            raise web.HTTPBadRequest(reason="Missed query paraemeter `query`")
        ts = await self.resolve_time_series(request)
        try:
            result = await ts.client.query(query)
            result = {"status": "ok", "result": result}
        except asyncio.CancelledError:
            raise
        except Exception as e:
            result = {"status": "error", "exception": str(e)}
        return web.json_response(result)

    async def _storage_raw_query(self, request):
        host = request.match_info["host"]
        url = f"http://{host}:{self.config.time_series_port}/query"
        subrequest = self._raw_storage_session.request(
            request.method,
            url,
            params=request.query,
            data=await request.post(),
        )
        async with subrequest as response:
            return web.Response(
                status=response.status,
                content_type=response.content_type,
                body=await response.read(),
            )

    async def _healthcheck(self, request):
        return web.json_response(dict(healthy=True, version=self._version))
