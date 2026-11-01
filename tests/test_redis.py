import asyncio
import multiprocessing
import os
import shutil
import uuid
from concurrent.futures import ProcessPoolExecutor

import pytest
from wred import Request

from wred_ratelimit import Limiter


def consume_in_worker(url, namespace, strategy):
    async def consume():
        async with Limiter.redis(
            url, namespace=namespace, strategy=strategy
        ) as limiter:
            decisions = await asyncio.gather(
                *(
                    limiter.check("17/minute", "shared", bucket="orders")
                    for _ in range(50)
                )
            )
            return sum(allowed for allowed, _ in decisions)

    return asyncio.run(consume())


@pytest.mark.parametrize("strategy", ["moving", "fixed"])
def test_quota_is_shared_across_processes(strategy):
    url = os.getenv("WRED_TEST_REDIS")
    if not url:
        pytest.skip("Set WRED_TEST_REDIS for real Redis integration")
    namespace = uuid.uuid4().hex
    with ProcessPoolExecutor(
        max_workers=4, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        futures = [
            pool.submit(consume_in_worker, url, namespace, strategy) for _ in range(4)
        ]
        assert sum(future.result(timeout=20) for future in futures) == 17


async def test_recovers_after_redis_restart(tmp_path):
    executable = shutil.which("redis-server")
    if not executable:
        pytest.skip("redis-server is required for restart integration")
    socket = tmp_path / "redis.sock"
    process = None

    async def start():
        nonlocal process
        process = await asyncio.create_subprocess_exec(
            executable,
            "--port",
            "0",
            "--unixsocket",
            str(socket),
            "--save",
            "",
            "--appendonly",
            "no",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        async with asyncio.timeout(5):
            while not socket.exists():
                if process.returncode is not None:
                    raise RuntimeError("Test Redis failed to start")
                await asyncio.sleep(0.01)

    async def stop():
        if process is not None and process.returncode is None:
            process.terminate()
            await asyncio.wait_for(process.wait(), 5)
        socket.unlink(missing_ok=True)

    try:
        await start()
        async with Limiter.redis(f"redis+unix://{socket}") as limiter:

            @limiter.limit("1/minute")
            async def handler(request):
                return {"ok": True}

            request = Request({"path": "/", "method": "GET"}, None, None)
            assert await handler(request) == {"ok": True}
            assert (await handler(request))[2] == 429
            await stop()
            assert (await handler(request))[2] == 503
            await start()
            # This server has no persistence: reconnect works, but quotas reset.
            assert await handler(request) == {"ok": True}
            assert (await handler(request))[2] == 429
    finally:
        await stop()
