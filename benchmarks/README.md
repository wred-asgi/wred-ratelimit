# Local Redis baseline

Measured on 2026-11-01 using Python 3.14.4, limits 5.8.0, redis-py 8.1.0,
Redis Server 8.0.5, and an Intel Core Ultra 7 256V host. Each scenario used a fresh
Redis process over a Unix socket with persistence disabled. Four worker processes
ran 25 concurrent decorated-handler calls each. No HTTP transport, TLS, proxy,
authentication, or application workload was included.

Each scenario ran 1,000,000 calls. The mixed workload used 10,000 identities and
a quota of 25/hour: exactly 250,000 allowed and 750,000 denied. The all-allowed
workload used 100,000 identities and a quota of 10/hour. All four runs matched
the expected quota counts, with no 503 responses. Distinct workers shared identities.

| Strategy | Workload | Duration | Calls/sec | p95 ms | p99 ms | Redis memory increase |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Moving | Mixed | 23.479 s | 42,591 | 3.710 | 3.770 | 8.80 MiB |
| Moving | All allowed | 18.179 s | 55,008 | 2.012 | 2.563 | 48.65 MiB |
| Fixed | Mixed | 38.131 s | 26,225 | 4.551 | 4.677 | 2.54 MiB |
| Fixed | All allowed | 16.447 s | 60,800 | 1.705 | 1.727 | 23.47 MiB |

Memory increase is Redis `used_memory` after completion minus the idle-server
baseline, not total process RSS or peak memory. The raw measurements include Redis
peak memory in [results-2026-11-01.json](results-2026-11-01.json). Each run leaves
its uniquely namespaced quota keys for one hour; use a dedicated benchmark server.

Reproduce the two workloads for each strategy, with `REDIS_URL` pointing at a fresh
disposable Redis instance:

```sh
python benchmarks/redis_load.py --strategy moving --requests 1000000 \
    --workers 4 --concurrency 25 --identities 10000 --quota 25
python benchmarks/redis_load.py --strategy moving --requests 1000000 \
    --workers 4 --concurrency 25 --identities 100000 --quota 10
```

Repeat with `--strategy fixed`. The script reports handler latency and quota results;
memory measurements above were collected separately through Redis INFO before and
after each run.

These are single, short, closed-loop local measurements. They are not steady-state
capacity limits, an overload test, or a production SLO. Requests wait for earlier
requests in each concurrency lane, so the latency distribution excludes queueing
that would occur under sustained offered traffic above capacity. Mixed workloads
transition from admission to rejection as quotas fill. Validate the complete service
with realistic network latency, key distributions, bursts, expiry cycles, Redis
failover, and application work before using these numbers for sizing.

The correctness suite separately exercises cross-process quotas, stalled connections,
pool exhaustion, and restart recovery. It passes on Python 3.11.11 with minimum
dependencies (limits 4.2, redis-py 5.2), Python 3.13.2 with minimum and intermediate
dependencies, and Python 3.14.4 with current dependencies. On 3.11, cancellation
during Redis connection startup was delayed until the configured timeout; see the
runtime caveat in the main README. Restart recovery does not preserve quota state
when Redis has no persistence, and is not a replicated-failover continuity test.
