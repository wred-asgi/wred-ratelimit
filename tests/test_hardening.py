import asyncio
import logging

import pytest
from limits.aio.storage import MemoryStorage
from wred import Request

from wred_ratelimit import Limiter


def request():
    return Request(
        {"path": "/", "method": "GET", "client": ("127.0.0.1", 1)}, None, None
    )


@pytest.mark.parametrize("strategy", ["moving", "fixed"])
async def test_admission_survives_stats_failure_without_bypassing_denial(
    strategy, caplog
):
    class StatsUnavailable(MemoryStorage):
        async def get_moving_window(self, *args, **kwargs):
            raise ValueError("stats unavailable")

        async def get_expiry(self, *args, **kwargs):
            raise ValueError("stats unavailable")

    async with Limiter(
        storage=StatsUnavailable(), strategy=strategy, fail_open=True
    ) as limiter:

        @limiter.limit("1/minute")
        async def handler(request):
            return {"ok": True}

        with caplog.at_level(logging.WARNING, logger="wred_ratelimit"):
            assert await handler(request()) == {"ok": True}
            assert not caplog.records
            denied = await handler(request())
        assert denied[2] == 429
        assert dict(denied[1])["retry-after"] == "60"
        assert caplog.records[-1].error_type == "ValueError"


@pytest.mark.parametrize("fail_open", [False, True])
async def test_programming_errors_propagate(fail_open):
    class BrokenStorage(MemoryStorage):
        async def acquire_entry(self, *args, **kwargs):
            raise TypeError("programming error")

    async with Limiter(storage=BrokenStorage(), fail_open=fail_open) as limiter:

        @limiter.limit("1/minute")
        async def handler(request):
            return {"ok": True}

        with pytest.raises(TypeError, match="programming error"):
            await handler(request())


@pytest.fixture
async def silent_redis():
    connected = asyncio.Event()
    writers = []

    async def unresponsive(reader, writer):
        writers.append(writer)
        connected.set()
        await reader.read()

    server = await asyncio.start_server(unresponsive, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"redis://127.0.0.1:{port}", connected
    finally:
        server.close()
        for writer in writers:
            writer.close()
            await writer.wait_closed()
        await server.wait_closed()


@pytest.mark.parametrize("fail_open", [False, True])
async def test_slow_redis_is_bounded_and_observable(fail_open, caplog, silent_redis):
    url, _ = silent_redis
    async with Limiter.redis(
        url,
        fail_open=fail_open,
        timeout=0.05,
        storage_options={"socket_timeout": 5, "socket_connect_timeout": 5},
    ) as limiter:

        @limiter.limit("1/minute", key=lambda request: "private-identity")
        async def handler(request):
            return {"ok": True}

        with caplog.at_level(logging.WARNING, logger="wred_ratelimit"):
            result = await asyncio.wait_for(handler(request()), timeout=1)
        if fail_open:
            assert result == {"ok": True}
        else:
            assert result[2] == 503
        assert caplog.records[-1].error_type == "TimeoutError"
        assert caplog.records[-1].fail_open is fail_open
        assert "private-identity" not in caplog.text
        async with asyncio.timeout(1):
            with pytest.raises(TimeoutError):
                await limiter.check("1/minute", "user", bucket="test")


async def test_redis_pool_is_bounded_and_cancellation_propagates(caplog, silent_redis):
    url, connected = silent_redis
    pending = None
    try:
        async with Limiter.redis(
            url,
            timeout=10,
            storage_options={"max_connections": 1, "socket_timeout": 10},
        ) as limiter:

            @limiter.limit("1/minute")
            async def handler(request):
                return {"ok": True}

            pending = asyncio.create_task(handler(request()))
            await asyncio.wait_for(connected.wait(), 1)
            with caplog.at_level(logging.WARNING, logger="wred_ratelimit"):
                result = await asyncio.wait_for(handler(request()), 1)
            assert result[2] == 503
            assert not pending.done()
            assert caplog.records[-1].error_type in (
                "ConnectionError",
                "MaxConnectionsError",
            )
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
