# wred-ratelimit

Async per-handler quotas using `limits`. Python 3.11+, Wred 0.13–0.15.

```sh
pip install 'wred-ratelimit[redis]'
```

```python
from wred import App
from wred_ratelimit import Limiter

app = App()
limiter = Limiter.redis("redis://localhost:6379/0", namespace="orders-api")


@app.route("/orders/{order_id}")
@limiter.limit("30/minute")
async def order(request, order_id):
    return {"id": order_id}
```

`Limiter(storage=None, namespace='wred', strategy='moving', key=client_ip,
fail_open=False, timeout=0.5)` uses an in-process store unless provided a limits
async store. Choose `moving` or `fixed` windows. The memory store belongs to each
limiter instance; use shared Redis for quotas across workers. All workers must
share a namespace, Redis database, bucket, and rule for shared quotas.

`Limiter.redis(url, storage_options=None, **options)` configures redis-py.
`options` configure the limiter; `storage_options` configure the limits Redis
storage and its client. Requires `limits>=4.2` and `redis>=5.2`.

```python
limiter = Limiter.redis(
    "redis://localhost:6379/0",
    namespace="orders-api",
    timeout=0.15,  # total storage budget per admission/check, in seconds
    storage_options={
        "socket_connect_timeout": 0.1,
        "socket_timeout": 0.1,
        "max_connections": 50,  # per limiter instance, normally per worker
    },
)
```

Defaults are a 0.5-second total storage deadline, 0.2-second connect/read timeouts,
100 connections per pool, and no automatic command retries. Tune these to your
application's latency and concurrency budget. Redis URL query parameters take
precedence over equivalent client options. A supplied `connection_pool` owns its
own client settings. The total deadline still applies. A timeout can occur after
Redis consumed quota but before the response arrived; automatically retrying a
mutation can charge twice. Override the retry policy only with that tradeoff in mind.

Use Python 3.12+ for latency-sensitive deployments. Python 3.11 remains compatible,
but a connection-startup cancellation test with redis-py 5.2 showed cancellation
delayed until the next timeout. This is consistent with the upstream
[asyncio cancellation race](https://github.com/python/cpython/issues/86296).
Deadlines rely on cooperative cancellation; keep socket timeouts short as a second
bound, especially on 3.11 or with custom storage implementations.

`limit(rule, key=None, bucket=None)` supports stacked rules. The default bucket is
handler module + qualified name; set `bucket` explicitly when reusing a handler for
distinct routes. Rejected requests consume outer quotas in a decorator stack.
Keys may be sync or async request callbacks and are hashed before storage.

The default identity uses the ASGI peer address, ignoring forwarded headers. Configure
trusted-proxy handling in the server before using client IPs. A user/API-key callback
must return a stable identity. Authentication must run before a user-based quota;
place a separate IP quota outside authentication to limit failed attempts.

Exhaustion returns JSON 429 and `Retry-After`. Storage failures and deadlines return
503 unless `fail_open=True`. Once Redis denies a request, a failure fetching reset
statistics preserves the 429, using a full window as a conservative `Retry-After`.
Allowed HTTP requests do not fetch statistics. Rules are parsed when decorated.

`await limiter.check(rule, identity, bucket='...')` consumes quota and returns
`(allowed, retry_after)`, including reset information for allowed calls. It propagates
storage failures and `TimeoutError` for service-level callers. The deadline covers
storage operations, not identity callbacks or the handler. Unexpected errors outside
the storage backend's declared exception types propagate; cancellation also propagates.

The decorator logs handled failures at WARNING to `wred_ratelimit`, with record fields
`namespace`, `bucket`, `fail_open`, `error_type`, and `quota_denied`. Configure your log
formatter/exporter to include these fields. Identity values, Redis URLs, and exception
messages are omitted. Monitor these events alongside HTTP 429/503 rates, request tail
latency, Redis memory, and pool/connection usage. Direct `check()` callers own their
error reporting. Avoid identities as metric labels.

Use `async with limiter` or `await limiter.aclose()` in the hosting application's
resource lifecycle. Wred itself does not implement lifespan.

Tests: `pip install 'wred-ratelimit[test,redis]'`, then `pytest`.
Set `WRED_TEST_REDIS=redis://localhost:6379/15` to include real Redis tests.
Use a dedicated test instance/database. Cross-process tests verify shared quotas
for both strategies. The restart test starts its own disposable Redis and requires
the `redis-server` executable. CI runs real Redis with minimum and current dependencies.

For a repeatable local load check, install the project and run:

```sh
REDIS_URL=redis://localhost:6379/15 python benchmarks/redis_load.py \
    --workers 4 --concurrency 25 --requests 20000 --identities 1000 --quota 5
```

Use a dedicated benchmark Redis. The script exits unsuccessfully on an incorrect
quota result or storage-error responses and prints throughput and p50/p95/p99
latency. It exercises decorated handlers across processes, without HTTP network or
application work. Each run creates a unique namespace whose keys expire in one hour.
Repeat with `--strategy fixed` and with many distinct identities to assess memory
and latency for your workload. This is a closed-loop load check, not an overload or
production-capacity certification; validate your full application under sustained
offered traffic and Redis failure/recovery before rollout.
See [the recorded local baseline](benchmarks/README.md) for measured results and limits.

Redis restarts without persistence, eviction, and failover that loses recent writes
can reset quotas. The restart test explicitly verifies recovery after state loss;
it does not promise quota continuity through failover. Choose persistence, memory
limits/eviction policy, and availability settings for your enforcement requirements.
Moving windows retain request timestamps and need more memory than fixed counters.
Keep worker clocks synchronized. Cluster/Sentinel deployments can supply an appropriate
limits async storage via `Limiter(storage=...)`; validate that topology separately.

## Extension compatibility

Follows the [Wred extension contract v1.1](https://github.com/wred-asgi/.github/blob/main/EXTENSIONS.md).
Keep `@app.route` outermost for
route decorators; register routes on the inner Wred app when using ASGI wrappers.
