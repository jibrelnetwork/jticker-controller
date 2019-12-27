import collections
import re
import asyncio
from functools import partial
from dataclasses import dataclass
from typing import Any

import pytest
from addict import Dict
from aiohttp import ClientSession, web

from jticker_core import injector, WebServer

from jticker_controller.controller import Controller
from jticker_controller import controller as controller_module


class _FakeKafka:

    def __init__(self):
        self.data = collections.defaultdict(list)
        self.subs = collections.defaultdict(set)

    def subscribe(self, topic, queue, from_beginning=False):
        self.subs[topic].add(queue)

    def set_offset(self, topic, queue, offset):
        for i, data in enumerate(self.data[topic], start=1):
            if i >= offset:
                queue.put_nowait((topic, i, data))

    def put(self, topic, data):
        self.data[topic].append(data)
        offset = len(self.data[topic])
        for q in self.subs[topic]:
            q.put_nowait((topic, offset, data))


class _FakeAioKafkaProducer:

    class Batch:

        def __init__(self):
            self.data = []
            self.closed = False

        def append(self, key, value, timestamp):
            assert not self.closed
            assert isinstance(key, bytes)
            assert isinstance(value, bytes)
            self.data.append((key, value, timestamp))

        def record_count(self):
            return len(self.data)

        def close(self):
            self.closed = True

    def __init__(self, _fake_kafka, loop, bootstrap_servers, **_):
        self._fake_kafka = _fake_kafka
        self.flush_called = asyncio.Future()

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, topic, value=None, key=None):
        assert re.match("^[a-zA-Z0-9\\._\\-]+$", topic)
        assert isinstance(value, str)
        assert isinstance(key, str)
        self._fake_kafka.put(topic, value)

    def create_batch(self):
        return self.Batch()

    async def send_batch(self, batch, topic, partition):
        assert isinstance(partition, int)
        assert isinstance(batch, self.Batch)
        for k, v, t in batch.data:
            await self.send_and_wait(topic, value=v.decode(), key=k.decode())

    async def send(self, topic, value=None, key=None):
        return await self.send_and_wait(topic, value, key)

    async def flush(self):
        self.flush_called.set_result(True)

    async def partitions_for(self, topic):
        assert isinstance(topic, str)
        return [0]


class _FakeAioKafkaConsumer:

    def __init__(self, *topics, _fake_kafka, loop, bootstrap_servers, auto_offset_reset="latest",
                 group_id=None):
        self._fake_kafka = _fake_kafka
        self.auto_offset_reset = auto_offset_reset
        self.q = asyncio.Queue()
        self._offsets = collections.defaultdict(int)
        self.subscribe(topics)

    async def start(self):
        pass

    async def stop(self):
        pass

    @dataclass
    class Message:
        value: Any

    async def __aiter__(self):
        for topic, offset in self._offsets.items():
            self._fake_kafka.set_offset(topic, self.q, offset)
        while True:
            topic, offset, value = await self.q.get()
            self._offsets[topic] = offset
            yield self.Message(value)

    def assign(self, partitions):
        self.subscribe([p.topic for p in partitions])

    def subscribe(self, topics):
        for subs in self._fake_kafka.subs.values():
            subs.discard(self.q)
        for topic in topics:
            self._fake_kafka.subscribe(topic, self.q)
            if self.auto_offset_reset == "earliest":
                self._offsets[topic] = 0

    async def end_offsets(self, partitions):
        offsets = {}
        for p in partitions:
            offsets[p] = len(self._fake_kafka.data.get(p.topic))
        return offsets

    async def seek_to_beginning(self, partition):
        self._offsets[partition.topic] = 0

    async def position(self, partition):
        return self._offsets.get(partition.topic, 0)


@pytest.fixture(autouse=True)
def mocked_kafka(monkeypatch):
    fake_kafka = _FakeKafka()
    with monkeypatch.context() as m:
        m.setattr(controller_module, "AIOKafkaProducer",
                  partial(_FakeAioKafkaProducer, _fake_kafka=fake_kafka))
        m.setattr(controller_module, "AIOKafkaConsumer",
                  partial(_FakeAioKafkaConsumer, _fake_kafka=fake_kafka))
        yield fake_kafka


@pytest.fixture(scope="session")
def _influx_config():
    return Dict(
        influx_host="127.0.0.1",
        influx_port="8086",
        influx_db="db",
        influx_username=None,
        influx_password=None,
        influx_ssl=False,
        influx_unix_socket=None,
        influx_chunk_size="1",
    )


@pytest.fixture(autouse=True, scope="session")
def config(_influx_config):
    config = Dict(
        add_candles_batch_size="10",
        add_candles_batch_timeout="60",
        kafka_bootstrap_servers="foo,bar,baz",
        kafka_tasks_topic="grabber_tasks",
        kafka_trading_pairs_topic="assets_metadata",
        time_series_host="127.0.0.1",
        time_series_port="8086",
        time_series_allow_migrations=True,
        time_series_default_row_limit="1000",
        time_series_client_timeout="10",
        web_host="127.0.0.1",
        web_port="8080",
        **_influx_config,
    )
    injector.register(lambda: config, name="config")
    injector.register(lambda: "TEST_VERSION", name="version")
    return config


@pytest.fixture
async def _web_app():
    return web.Application()


@pytest.fixture
async def _web_server(_web_app):
    return WebServer(web_app=_web_app)


@pytest.fixture
async def controller(_web_server, time_series, clean_influx):
    async with Controller(web_server=_web_server, time_series=time_series) as controller:
        yield controller


@pytest.fixture
def base_url(config):
    host = config.web_host
    port = config.web_port
    return f"http://{host}:{port}"


@pytest.fixture
async def client(base_url, controller):
    async def request(method, path, *, _raw=False, **json):
        url = f"{base_url}/{path}"
        async with session.request(method, url, json=json) as response:
            await response.read()
            if _raw:
                return response
            else:
                assert response.status == 200
                return await response.json()
    async with ClientSession() as session:
        yield request
