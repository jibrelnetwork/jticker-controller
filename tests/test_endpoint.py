import pytest

from jticker_core import Task


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
