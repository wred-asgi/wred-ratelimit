import asyncio
import os
import uuid

import httpx
import pytest
from wred import App

from wred_ratelimit import Limiter


@pytest.fixture(params=["memory", "redis"])
async def limiter(request):
    kwargs = {"namespace": uuid.uuid4().hex}
    if request.param == "redis":
        url = os.getenv("WRED_TEST_REDIS")
        if not url:
            pytest.skip("Set WRED_TEST_REDIS for real Redis integration")
        result = Limiter.redis(url, **kwargs)
    else:
        result = Limiter(**kwargs)
    yield result
    await result.aclose()


async def test_concurrent_quota_and_identity(limiter):
    result = await asyncio.gather(
        *(limiter.check("5/minute", "alice", bucket="orders") for _ in range(20))
    )
    assert sum(allowed for allowed, _ in result) == 5
    assert all(retry > 0 for _, retry in result)
    assert (await limiter.check("5/minute", "bob", bucket="orders"))[0]
    assert (await limiter.check("5/minute", "alice", bucket="elsewhere"))[0]


async def test_http_path_forwarding(limiter):
    app = App()

    @app.route("/{name}")
    @limiter.limit("1/minute")
    async def handler(request, name):
        return {"name": name}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        assert (await client.get("/alice")).json() == {"name": "alice"}
        result = await client.get("/bob", headers={"X-Forwarded-For": "1.2.3.4"})
        assert result.status_code == 429 and int(result.headers["retry-after"]) > 0


async def test_expiry_and_fixed_window():
    limiter = Limiter(strategy="fixed")
    assert (await limiter.check("1/second", "user", bucket="test"))[0]
    assert not (await limiter.check("1/second", "user", bucket="test"))[0]
    await asyncio.sleep(1.05)
    assert (await limiter.check("1/second", "user", bucket="test"))[0]


async def test_storage_outage():
    limiter = Limiter.redis("redis://127.0.0.1:1")

    @limiter.limit("1/minute")
    async def handler(request):
        return {"ok": True}

    from wred import Request

    request = Request({"path": "/", "method": "GET"}, None, None)
    assert (await handler(request))[2] == 503
    limiter.fail_open = True
    assert await handler(request) == {"ok": True}
    await limiter.aclose()
