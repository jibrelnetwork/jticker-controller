import pytest
import pickle

from aiohttp import ClientSession
from async_timeout import timeout
from aioinflux import iterpoints

from jticker_core import Task, EPOCH_START, Candle, Interval, TradingPair


@pytest.mark.asyncio
async def test_add_task(client, mocked_kafka):
    await client("POST", "task/add", _raw=True, providers=["ex"], symbols=["BTCUSD"])
    assert len(mocked_kafka.data) == 1
    assert "grabber_tasks" in mocked_kafka.data
    tasks = mocked_kafka.data["grabber_tasks"]
    assert len(tasks) == 1
    t = Task.from_json(tasks[0])
    assert t.providers == ["ex"]
    assert t.symbols == ["BTCUSD"]


@pytest.mark.asyncio
async def test_healthcheck(client):
    d = await client("GET", "healthcheck")
    assert d == dict(healthy=True, version="TEST_VERSION")


@pytest.mark.asyncio
async def test_strip(client, mocked_kafka, controller):
    ms = [
        {
            "measurement": "mapping",
            "fields": {
                "exchange": "EX",
                "symbol": "AB",
                "measurement": "EX_AB_random",
            }
        },
        {
            "measurement": "EX_AB_random",
            "time": (EPOCH_START - 1) * 10 ** 9,
            "tags": {"interval": "60"},
            "fields": {"fake": 1},
        },
        {
            "measurement": "EX_AB_random",
            "time": (EPOCH_START) * 10 ** 9,
            "tags": {"interval": "666"},
            "fields": {"fake": 2},
        },
        {
            "measurement": "EX_AB_random",
            "time": (EPOCH_START + 86400) * 10 ** 9,
            "tags": {"interval": "666"},
            "fields": {"fake": 3},
        },
        {
            "measurement": "EX_AB_random",
            "time": (EPOCH_START + 86400) * 10 ** 9,
            "tags": {"interval": "60"},
            "fields": {"fake": 4},
        },
    ]
    assert len(controller.influx_clients) == 1
    influx_client = controller.influx_clients[0]
    response = await influx_client.query("show measurements")
    points = list(iterpoints(response))
    assert not points
    await influx_client.write(ms)
    response = await influx_client.query("show measurements")
    points = list(iterpoints(response))
    assert len(points) == 2
    response = await influx_client.query("select * from EX_AB_random")
    points = list(iterpoints(response))
    assert len(points) == 4
    await controller._strip()
    response = await influx_client.query("select * from EX_AB_random")
    points = list(iterpoints(response))
    assert len(points) == 2
    assert set(map(tuple, points)) == {
        ((EPOCH_START) * 10 ** 9, 2, "666"),
        ((EPOCH_START + 86400) * 10 ** 9, 4, "60"),
    }


@pytest.mark.asyncio
async def test_add_candles(base_url, mocked_kafka, controller):
    c = Candle(
        exchange="exchange",
        symbol="AB",
        interval=Interval.MIN_1,
        open=1,
        high=2,
        low=0,
        close=1,
        timestamp=EPOCH_START + 1,
    )
    data = [c.as_dict()] * 1001
    assert not mocked_kafka.data
    async with ClientSession() as session:
        async with session.ws_connect(f"{base_url}/ws/add_candles") as ws:
            await ws.send_json(data)
            c.interval = Interval.M_3
            await ws.send_json([c.as_dict()])
    assert len(mocked_kafka.data) == 2
    assert set(map(len, mocked_kafka.data.values())) == {1, 1001}
    await controller._producer.flush_called


@pytest.mark.asyncio
async def test_get_candles(base_url, mocked_kafka, controller):
    c = Candle(
        exchange="exchange",
        symbol="AB",
        interval=Interval.MIN_1,
        open=1,
        high=2,
        low=0,
        close=1,
        timestamp=EPOCH_START + 1,
    )
    mocked_kafka.put("assets_metadata", TradingPair(symbol=c.symbol, exchange=c.exchange).as_json())
    mocked_kafka.put("exchange_AB_60", c.as_json())
    async with ClientSession() as session:
        async with session.get(f"{base_url}/list_topics") as response:
            topics = await response.json()
            assert topics == ["exchange_AB_60"]
        async with session.ws_connect(f"{base_url}/ws/get_candles") as ws:
            async with timeout(1):
                await ws.send_json("exchange_AB_60")
                msgs = []
                async for msg in ws:
                    msgs.append(msg)
                assert len(msgs) == 2
                data = pickle.loads(msgs[0].data)
                assert c == Candle.from_json(data[0].value)
                assert msgs[1].data == b""
