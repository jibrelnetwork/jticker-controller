import asyncio
import uuid
import collections
import random
import pickle
import time
from functools import partial

import backoff
import mujson as json
from aiohttp import web
from mode import Service
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition
from aiokafka.errors import ConnectionError as KafkaConnectionError
from addict import Dict
from aioinflux import InfluxDBClient, iterpoints
from loguru import logger
from tqdm import tqdm

from jticker_core import (inject, register, WebServer, Task, EPOCH_START, TradingPair, Interval,
                          Rate, TqdmLogFile, normalize_kafka_topic, Candle)


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
    def __init__(self, web_server: WebServer, config: Dict, version: str):
        super().__init__()
        self.web_server = web_server
        self._bootstrap_servers = config.kafka_bootstrap_servers.split(",")
        self._producer = AIOKafkaProducer(
            loop=asyncio.get_event_loop(),
            bootstrap_servers=self._bootstrap_servers,
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
        self._assets_metadata_topic = config.kafka_trading_pairs_topic
        self._add_candles_batch_size = int(config.add_candles_batch_size)
        self._add_candles_batch_timeout = int(config.add_candles_batch_timeout)
        self._version = version
        self._configure_router()

    def _configure_router(self):
        self.web_server.app.router.add_route("POST", "/task/add", self._add_task)
        self.web_server.app.router.add_route("GET", "/storage/strip", self._schedule_strip)
        self.web_server.app.router.add_route("GET", "/storage/query", self._storage_query)
        self.web_server.app.router.add_route("GET", "/healthcheck", self._healthcheck)
        self.web_server.app.router.add_route("GET", "/ws/add_candles", self.add_candles_ws_handler)
        self.web_server.app.router.add_route("GET", "/list_topics", self.list_topics)
        self.web_server.app.router.add_route("GET", "/ws/get_candles", self.get_candles_ws_handler)

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

    async def _do_influx_query(self, query):
        coros = [c.query(query) for c in self.influx_clients]
        responses = await asyncio.gather(*coros)
        results = []
        for response in responses:
            rows = []
            for row in iterpoints(response, lambda *x, meta: dict(zip(meta['columns'], x))):
                rows.append(row)
            results.append(rows)
        return results

    async def _strip_alphavantage(self, topic):
        responses = await self._do_influx_query(f"""select * from {topic}
                                                    where open = 0 or high = 0 or
                                                        low = 0 or close = 0
                                                    order by time""")
        for r in responses:
            for zp in r:
                await self._do_influx_query(f"""delete from {topic}
                                                where time = {zp["time"]} and
                                                    interval = '{zp["interval"]}'""")

    async def _strip(self):
        # TODO: simplify when resolved
        # https://github.com/influxdata/influxdb/issues/9636
        logger.info("strip started")
        measurements = await self._do_influx_query("show measurements")
        topics = set()
        for points in measurements:
            for p in points:
                name = p["name"]
                if name != "mapping":
                    topics.add(name)
        total = len(topics)
        logger.info("will strip {} topics", total)
        epoch = int(EPOCH_START * 10 ** 9)
        for topic in tqdm(sorted(topics), ncols=80, ascii=True, mininterval=10,
                          maxinterval=10, file=TqdmLogFile(logger=logger)):
            responses = await self._do_influx_query(f"""select * from "{topic}"
                                                        order by time limit 1""")
            earliest = min((r[0]["time"] for r in responses if r), default=None)
            if earliest is None:
                continue
            # remove by epoch
            if earliest < epoch:
                await self._do_influx_query(f"""delete from "{topic}"
                                                where time < {epoch}""")
            # remove obsolete non-minute candles
            responses = await self._do_influx_query(f"""show tag values from "{topic}"
                                                        with key = "interval" """)
            if len(responses[0]) != 1:
                responses = await self._do_influx_query(f"""select * from "{topic}"
                                                            where interval = '60'
                                                            order by time limit 1""")
                earliest = min((r[0]["time"] for r in responses if r), default=None)
                if earliest is not None:
                    await self._do_influx_query(f"""delete from "{topic}"
                                                    where interval <> '60' and
                                                        time >= {earliest}""")

            # ad-hoc alphavantage strip, should be removed later
            if "alphavantage" in topic:
                await self._strip_alphavantage(topic)
        logger.info("strip done")

    async def _schedule_strip(self, request):
        logger.info("strip scheduled")
        self.add_future(self._strip())
        raise web.HTTPOk()

    async def _storage_query(self, request):
        query = request.query.get("query")
        if query is None:
            raise web.HTTPBadRequest("Missed query paraemeter `query`")
        try:
            result = await self._do_influx_query(query)
            result = {"status": "ok", "result": result}
        except asyncio.CancelledError:
            raise
        except Exception as e:
            result = {"status": "error", "exception": str(e)}
        return web.json_response(result)

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
        while batches:
            topic, batch = batches.popitem()
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
        last_candle_time_by_topic = {}
        async for msg in ws:
            for c in json.loads(msg.data):
                try:
                    # candle validation and normalization
                    c = Candle.from_dict(c).as_dict()
                except Exception:
                    logger.exception("can't build candle from {}", c)
                    continue
                if c["interval"] not in self.ALLOWED_CANDLE_INTERVALS:
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
                count += 1
                rate.inc()
                now = time.perf_counter()
                last_candle_time_by_topic[topic] = now
                if len(batch) >= self._add_candles_batch_size:
                    batches.pop(topic)
                    await self._batches_queue.put((topic, batch))
                for topic, batch in list(batches.items()):
                    last = last_candle_time_by_topic[topic]
                    if now - last > self._add_candles_batch_timeout:
                        batches.pop(topic)
                        await self._batches_queue.put((topic, batch))
        # can't do "await" things here, since cancelation
        # https://docs.aiohttp.org/en/stable/web_advanced.html#web-handler-cancellation
        self.add_future(self._finalize_batches(batches, count))
        return ws

    async def _read_kafka_asset_topics(self):
        topics = []
        c = AIOKafkaConsumer(
            loop=asyncio.get_running_loop(),
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset='earliest',
        )
        async with AsyncResourceContext(c) as consumer:
            logger.info("start loading assets...")
            partition = TopicPartition(self._assets_metadata_topic, 0)
            last_known_offset = (await consumer.end_offsets([partition]))[partition]
            logger.debug("last known assets metadata offset: {}", last_known_offset)
            if last_known_offset <= 0:
                logger.warning("no assets in kafka")
                return topics
            consumer.assign([partition])
            await consumer.seek_to_beginning(partition)
            async for msg in consumer:
                try:
                    data = json.loads(msg.value)
                except json.JSONDecodeError:
                    logger.error("can not decode trading pair from {!r}", msg.value)
                else:
                    topic = data.get("topic") or data.get("kafka_topic")
                    if topic is None:
                        logger.warning("can't resolve topic from {}", data)
                    else:
                        topics.append(topic)
                finally:
                    current_position = await consumer.position(partition)
                    if current_position >= last_known_offset:
                        break
        return topics

    async def list_topics(self, request):
        topics = await self._read_kafka_asset_topics()
        logger.info("found {} topics", len(topics))
        return web.json_response(topics)

    async def get_candles_ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        chunk = []
        topic = normalize_kafka_topic(await ws.receive_json())
        logger.info("dump topic {}", topic)
        c = AIOKafkaConsumer(
            topic,
            loop=asyncio.get_running_loop(),
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset='earliest',
        )
        async with AsyncResourceContext(c) as consumer:
            partition = TopicPartition(topic, 0)
            last_known_offset = (await consumer.end_offsets([partition]))[partition]
            current_position = await consumer.position(partition)
            if current_position < last_known_offset:
                async for msg in consumer:
                    chunk.append(msg)
                    if len(chunk) >= 100:
                        await ws.send_bytes(pickle.dumps(chunk))
                        chunk = []
                    current_position = await consumer.position(partition)
                    if current_position >= last_known_offset:
                        break
                if chunk:
                    await ws.send_bytes(pickle.dumps(chunk))
        # eof token
        await ws.send_bytes(b"")
        return ws
