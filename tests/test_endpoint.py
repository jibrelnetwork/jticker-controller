import pytest
import pickle
import asyncio

from aiohttp import ClientSession
from async_timeout import timeout

from jticker_core import Task, EPOCH_START, Candle, Interval, RawTradingPair


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
async def test_add_candles(base_url, mocked_kafka, controller):
    c = Candle(
        exchange="exchange",
        symbol="AB",
        interval=Interval.MIN_1,
        open=1,
        high=2,
        low=0.1,
        close=1,
        timestamp=EPOCH_START + 1,
    )
    data = [c.to_dict(encode_json=True)] * 1001
    assert not mocked_kafka.data
    async with ClientSession() as session:
        async with session.ws_connect(f"{base_url}/ws/add_candles") as ws:
            await ws.send_json(data)
            c.interval = Interval.M_3
            await ws.send_json([c.to_dict(encode_json=True)])
            c.interval = Interval.MIN_1
            c.high, c.low = c.low, c.high
            await ws.send_json([c.to_dict(encode_json=True)])
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
        low=0.1,
        close=1,
        timestamp=EPOCH_START + 1,
    )
    tp = RawTradingPair(symbol=c.symbol, exchange=c.exchange).to_json()
    mocked_kafka.put("assets_metadata", tp)
    mocked_kafka.put("exchange_AB_60", c.to_json())
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


@pytest.mark.asyncio
async def test_storage_query(controller, client):
    response = await client("GET", "storage/query?query=show%20databases")
    assert response["status"] == "ok"
    result = response["result"]
    assert result["0"] == []
    await client("GET", "storage/query?query=create%20database%20foo")
    response = await client("GET", "storage/query?query=show%20databases")
    assert response["status"] == "ok"
    result = response["result"]
    assert result["0"] == [{"name": "foo"}]

    response = await client("GET", "storage/query", _raw=True)
    assert response.status == 400

    response = await client("GET", "storage/query?query=foobar")
    assert response["status"] == "error"


@pytest.mark.asyncio
async def test_migration(controller, client):
    await client("GET", "storage/migrate", _raw=True)
    async with timeout(10):
        while True:
            await asyncio.sleep(0.1)
            names = set(await controller.time_series.client.show_databases())
            if names == {"service", "candles"}:
                break
