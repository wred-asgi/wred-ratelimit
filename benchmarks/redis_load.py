"""Exercise decorated handlers in independent processes against a disposable Redis.

No HTTP server/network is included. Each run uses a unique namespace; its keys
expire after one hour. REDIS_URL must point at a dedicated benchmark instance.
"""

import argparse
import asyncio
import json
import math
import multiprocessing
import os
import time
import uuid
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

from wred import Request

from wred_ratelimit import Limiter


def worker(index, options, namespace):
    async def run():
        latencies = []
        statuses = Counter()
        per_worker, remainder = divmod(options["requests"], options["workers"])
        first = index * per_worker + min(index, remainder)
        indices = iter(range(first, first + per_worker + (index < remainder)))
        async with Limiter.redis(
            os.environ["REDIS_URL"],
            namespace=namespace,
            strategy=options["strategy"],
            timeout=options["timeout"],
            storage_options={"max_connections": options["concurrency"]},
        ) as limiter:

            @limiter.limit(f"{options['quota']}/hour", bucket="load")
            async def handler(request):
                return {}, [], 200

            async def lane():
                for number in indices:
                    request = Request(
                        {
                            "path": "/",
                            "method": "GET",
                            "client": (str(number % options["identities"]), 1),
                        },
                        None,
                        None,
                    )
                    start = time.perf_counter()
                    result = await handler(request)
                    latencies.append((time.perf_counter() - start) * 1000)
                    statuses[result[2]] += 1

            start = time.perf_counter()
            await asyncio.gather(*(lane() for _ in range(options["concurrency"])))
            end = time.perf_counter()
        return dict(statuses), latencies, start, end

    return asyncio.run(run())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--concurrency", type=int, default=25, help="Concurrent calls per worker"
    )
    parser.add_argument("--requests", type=int, default=20000)
    parser.add_argument("--identities", type=int, default=1000)
    parser.add_argument(
        "--quota", type=int, default=5, help="Per-identity hourly quota"
    )
    parser.add_argument("--strategy", choices=["moving", "fixed"], default="moving")
    parser.add_argument("--timeout", type=float, default=0.5)
    options = vars(parser.parse_args())
    if "REDIS_URL" not in os.environ:
        parser.error("Set REDIS_URL to a dedicated benchmark Redis instance")
    if any(
        options[key] <= 0
        for key in ("workers", "concurrency", "requests", "identities", "quota")
    ):
        parser.error("Counts must be positive")
    if not math.isfinite(options["timeout"]) or options["timeout"] <= 0:
        parser.error("Timeout must be finite and positive")

    namespace = "wred-benchmark-" + uuid.uuid4().hex
    with ProcessPoolExecutor(
        max_workers=options["workers"], mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        futures = [
            pool.submit(worker, index, options, namespace)
            for index in range(options["workers"])
        ]
        results = [future.result() for future in futures]
    statuses = Counter()
    latencies = []
    for counts, timings, _, _ in results:
        statuses.update(counts)
        latencies.extend(timings)
    latencies.sort()
    elapsed = max(result[3] for result in results) - min(
        result[2] for result in results
    )
    per_identity, remainder = divmod(options["requests"], options["identities"])
    expected = remainder * min(options["quota"], per_identity + 1) + (
        options["identities"] - remainder
    ) * min(options["quota"], per_identity)
    correct = (
        statuses[200] == expected and statuses[429] == options["requests"] - expected
    )
    print(
        json.dumps(
            {
                **options,
                "namespace": namespace,
                "elapsed_seconds": round(elapsed, 3),
                "requests_per_second": round(len(latencies) / elapsed),
                "latency_ms": {
                    f"p{percentile}": round(
                        latencies[math.ceil(len(latencies) * percentile / 100) - 1], 3
                    )
                    for percentile in (50, 95, 99)
                },
                "statuses": dict(statuses),
                "expected_allowed": expected,
                "quota_correct": correct,
            },
            indent=2,
        )
    )
    raise SystemExit(0 if correct else 1)


if __name__ == "__main__":
    main()
