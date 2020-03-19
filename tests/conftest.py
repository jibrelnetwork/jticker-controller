import pytest
from addict import Dict
from aiohttp import ClientSession, web

from jticker_controller.controller import Controller
from jticker_core import WebServer, injector


@pytest.fixture(autouse=True, scope="session")
def config():
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
    async with Controller(web_server=_web_server) as controller:
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
