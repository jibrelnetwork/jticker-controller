import collections
import re
from functools import partial

import pytest
from addict import Dict
from aiohttp import ClientSession, web

from jticker_core import injector

from jticker_controller.controller import Controller
from jticker_controller import controller as controller_module
from jticker_core import WebServer


class _FakeKafka:

    def __init__(self):
        self.data = collections.defaultdict(list)
        self.subs = collections.defaultdict(list)

    def subscribe(self, topic, queue):
        self.subs[topic].append(queue)

    def put(self, topic, data):
        self.data[topic].append(data)
        offset = len(self.data[topic])
        for q in self.subs[topic]:
            q.put_nowait((topic, offset, data))


class _FakeAioKafkaProducer:

    def __init__(self, _fake_kafka, loop, bootstrap_servers, **_):
        self._fake_kafka = _fake_kafka

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, topic, value=None, key=None):
        assert re.match("^[a-zA-Z0-9\\._\\-]+$", topic)
        self._fake_kafka.put(topic, value)


@pytest.fixture(autouse=True)
def mocked_kafka(monkeypatch):
    fake_kafka = _FakeKafka()
    with monkeypatch.context() as m:
        m.setattr(controller_module, "AIOKafkaProducer",
                  partial(_FakeAioKafkaProducer, _fake_kafka=fake_kafka))
        yield fake_kafka


@pytest.fixture(autouse=True)
def _injector(unused_tcp_port):
    config = Dict(
        kafka_bootstrap_servers="foo,bar,baz",
        kafka_tasks_topic="grabber_tasks",
    )
    injector.register(lambda: config, name="config")
    injector.register(lambda: "127.0.0.1", name="host")
    injector.register(lambda: str(unused_tcp_port), name="port")
    injector.register(lambda: "TEST_VERSION", name="version")
    return injector


@pytest.fixture
async def _web_app():
    return web.Application()


@pytest.fixture
async def _web_server(_web_app):
    return WebServer(web_app=_web_app)


@pytest.fixture
async def controller(_web_server):
    async with Controller(web_server=_web_server) as controller:
        yield controller


@pytest.fixture
async def client(_injector, controller):
    async def request(method, path, *, _raw=False, **json):
        url = f"http://{host}:{port}/{path}"
        async with session.request(method, url, json=json) as response:
            if _raw:
                return response
            else:
                return await response.json()
    host = _injector.get("host")
    port = _injector.get("port")
    async with ClientSession() as session:
        yield request
