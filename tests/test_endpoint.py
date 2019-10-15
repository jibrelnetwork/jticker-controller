import pytest

from aiohttp import ClientSession

from jticker_core import Task, EPOCH_START, Candle, Interval


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
    await client("GET", "storage/strip", _raw=True)
    await controller._strip()


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
    assert len(mocked_kafka.data) == 2
    assert set(map(len, mocked_kafka.data.values())) == {1, 1001}
    await controller._producer.flush_called
