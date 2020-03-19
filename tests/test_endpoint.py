import asyncio

import pytest
from async_timeout import timeout


@pytest.mark.asyncio
async def test_healthcheck(client):
    d = await client("GET", "healthcheck")
    assert d == dict(healthy=True, version="TEST_VERSION")


@pytest.mark.asyncio
async def test_storage_query(controller, client):
    response = await client("GET", "storage/localhost/query?query=show%20databases")
    assert response["status"] == "ok"
    result = response["result"]
    assert result["0"] == []
    await client("GET", "storage/localhost/query?query=create%20database%20foo")
    response = await client("GET", "storage/localhost/query?query=show%20databases")
    assert response["status"] == "ok"
    result = response["result"]
    assert result["0"] == [{"name": "foo"}]

    response = await client("GET", "storage/localhost/query", _raw=True)
    assert response.status == 400

    response = await client("GET", "storage/localhost/query?query=foobar")
    assert response["status"] == "error"


@pytest.mark.asyncio
async def test_migration(controller, client):
    await client("GET", "storage/localhost/migrate", _raw=True)
    async with timeout(10):
        while True:
            await asyncio.sleep(0.1)
            assert len(controller._time_series_by_host) == 1
            ts, *_ = controller._time_series_by_host.values()
            names = set(await ts.client.show_databases())
            if names == {"service", "candles"}:
                break
