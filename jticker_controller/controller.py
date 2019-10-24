import asyncio
import uuid
import datetime
import collections
import random
from functools import partial

import backoff
import mujson as json
from aiohttp import web
from mode import Service
from aiokafka import AIOKafkaProducer
from aiokafka.errors import ConnectionError as KafkaConnectionError
from addict import Dict
from aioinflux import InfluxDBClient, iterpoints
from loguru import logger
from tqdm import tqdm

from jticker_core import inject, register, WebServer, Task, EPOCH_START, TradingPair, Interval, Rate


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
        self._batches_queue: asyncio.Queue = asyncio.Queue(maxsize=10)
        self._task_topic = config.kafka_tasks_topic
        self._version = version
        self._configure_router()

    def _configure_router(self):
        self.web_server.app.router.add_route("POST", "/task/add", self._add_task)
        self.web_server.app.router.add_route("GET", "/storage/strip", self._schedule_strip)
        self.web_server.app.router.add_route("GET", "/healthcheck", self._healthcheck)
        self.web_server.app.router.add_route("GET", "/ws/add_candles", self.add_candles_ws_handler)

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
        self.add_future(self._batch_writer())

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
        for topic in tqdm(topics, ncols=80, ascii=True, mininterval=10,
                          maxinterval=10, file=LogFile(logger=logger)):
            coros = [c.query(f"""delete from "{topic}" where time < {int(EPOCH_START * 10 ** 9)}""")
                     for c in self.influx_clients]
            await asyncio.gather(*coros)
        logger.info("strip done")

    async def _schedule_strip(self, request):
        logger.info("strip scheduled")
        self.add_future(self._strip())
        raise web.HTTPOk()

    async def _healthcheck(self, request):
        return web.json_response(dict(healthy=True, version=self._version))

    async def _batch_writer(self):
        rate = Rate(log_period=15, log_template="outgoing rate: {:.3f} candles/s")
        while True:
            topic, candles = await self._batches_queue.get()
            partitions = await self._producer.partitions_for(topic)
            batch = self._producer.create_batch()
            for c in candles:
                meta = batch.append(
                    key=str(c["timestamp"]).encode("utf-8"),
                    value=json.dumps(c).encode("utf-8"),
                    timestamp=None,
                )
                if meta is None:
                    partition = random.choice(tuple(partitions))
                    await self._producer.send_batch(batch, topic, partition=partition)
                    batch = self._producer.create_batch()
                rate.inc()
            partition = random.choice(tuple(partitions))
            await self._producer.send_batch(batch, topic, partition=partition)
            self._batches_queue.task_done()

    async def _finalize_batches(self, batches, count):
        logger.info("enque partial batches...")
        for topic, batch in batches.items():
            await self._batches_queue.put((topic, batch))
        logger.info("awaiting queued batches stored...")
        await self._batches_queue.join()
        await self._producer.flush()
        logger.info("{} candles added to kafka", count)

    async def add_candles_ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        published_trading_pairs = {}
        count = 0
        logger.info("add candles ws opened")
        rate = Rate(log_period=15, log_template="incoming rate: {:.3f} candles/s")
        batches = collections.defaultdict(list)
        async for msg in ws:
            for c in json.loads(msg.data):
                # TODO: support multiple intervals
                if c["interval"] != Interval.MIN_1.value:
                    continue
                tp_key = exchange, symbol = c["exchange"], c["symbol"]
                if tp_key not in published_trading_pairs:
                    trading_pair_key_string = f"{exchange}:{symbol}"
                    topic = f"{exchange}_{symbol}_{c['interval']}"
                    trading_pair = TradingPair(symbol, exchange, topic=topic)
                    await self._producer.send(
                        "assets_metadata",
                        value=trading_pair.as_json(),
                        key=trading_pair_key_string,
                    )
                    published_trading_pairs[tp_key] = topic
                topic = published_trading_pairs[tp_key]
                batch = batches[topic]
                batch.append(c)
                if len(batch) >= 10_000:
                    batches.pop(topic)
                    await self._batches_queue.put((topic, batch))
                count += 1
                rate.inc()
        # can't do "await" things here, since cancelation
        # https://docs.aiohttp.org/en/stable/web_advanced.html#web-handler-cancellation
        self.add_future(self._finalize_batches(batches, count))
        return ws
