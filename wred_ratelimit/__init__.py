"""Per-handler request quotas using the async limits engine."""

import asyncio
import hashlib
import inspect
import logging
import math
import time
from functools import wraps

from limits import parse
from limits.aio.storage import MemoryStorage, RedisStorage
from limits.aio.strategies import FixedWindowRateLimiter, MovingWindowRateLimiter
from limits.errors import StorageError

__version__ = "0.1.1"
logger = logging.getLogger(__name__)


def client_ip(request):
    """Use the ASGI peer address; forwarded headers are not trusted."""
    return (request.scope.get("client") or ("unknown",))[0]


class Limiter:
    def __init__(
        self,
        *,
        storage=None,
        namespace="wred",
        strategy="moving",
        key=client_ip,
        fail_open=False,
        timeout=0.5,
    ):
        if strategy not in ("fixed", "moving"):
            raise ValueError("strategy must be fixed or moving")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a finite positive number of seconds")
        self.storage = storage if storage is not None else MemoryStorage()
        self.engine = (
            MovingWindowRateLimiter if strategy == "moving" else FixedWindowRateLimiter
        )(self.storage)
        self.namespace, self.key, self.fail_open = namespace, key, fail_open
        self.timeout = timeout
        errors = self.storage.base_exceptions
        if not isinstance(errors, tuple):
            errors = (errors,)
        self._storage_errors = (StorageError, TimeoutError, *errors)

    @classmethod
    def redis(cls, url, *, storage_options=None, **kwargs):
        """Create Redis storage with bounded connections and no command retries.

        ``storage_options`` are forwarded to the limits RedisStorage constructor.
        Remaining keywords configure this Limiter. Redis URL options take precedence
        over client keyword options, as in redis-py.
        """
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff

        options = {
            "socket_connect_timeout": 0.2,
            "socket_timeout": 0.2,
            "max_connections": 100,
            # A lost response does not mean the quota mutation failed. Retrying
            # could charge a request twice.
            "retry": Retry(NoBackoff(), 0),
        }
        options.update(storage_options or {})
        return cls(
            storage=RedisStorage(url, implementation="redispy", **options), **kwargs
        )

    def _identifiers(self, identity, bucket):
        return (
            self.namespace,
            bucket,
            hashlib.sha256(str(identity).encode()).hexdigest(),
        )

    async def _retry_after(self, item, identifiers):
        stats = await self.engine.get_window_stats(item, *identifiers)
        return max(1, math.ceil(stats.reset_time - time.time()))

    async def check(self, rule, identity, *, bucket):
        """Consume quota, returning (allowed, retry_after); storage errors propagate."""
        item = parse(rule)
        identifiers = self._identifiers(identity, bucket)
        async with asyncio.timeout(self.timeout):
            allowed = await self.engine.hit(item, *identifiers)
            return allowed, await self._retry_after(item, identifiers)

    def limit(self, rule, *, key=None, bucket=None):
        item = parse(rule)

        def decorate(handler):
            route_bucket = bucket or f"{handler.__module__}.{handler.__qualname__}"

            @wraps(handler)
            async def wrapped(request, *args, **kwargs):
                identity = (key or self.key)(request)
                if inspect.isawaitable(identity):
                    identity = await identity
                identifiers = self._identifiers(identity, route_bucket)
                allowed = None
                # If statistics fail after a denial, keep the denial and use a
                # conservative full window instead of allowing an exhausted quota.
                retry = item.get_expiry()
                try:
                    async with asyncio.timeout(self.timeout):
                        allowed = await self.engine.hit(item, *identifiers)
                        if not allowed:
                            retry = await self._retry_after(item, identifiers)
                except self._storage_errors as exc:
                    # Do not log identities, Redis URLs, or exception messages:
                    # they can contain credentials or other sensitive data.
                    logger.warning(
                        "Rate limit storage failure",
                        extra={
                            "namespace": self.namespace,
                            "bucket": route_bucket,
                            "fail_open": self.fail_open,
                            "error_type": type(exc).__name__,
                            "quota_denied": allowed is False,
                        },
                    )
                    if allowed is not False and not self.fail_open:
                        return (
                            {"detail": "Rate limit service unavailable"},
                            [("content-type", "application/json")],
                            503,
                        )
                if allowed is False:
                    return (
                        {"detail": "Rate limit exceeded"},
                        [
                            ("content-type", "application/json"),
                            ("retry-after", str(retry)),
                        ],
                        429,
                    )
                return await handler(request, *args, **kwargs)

            return wrapped

        return decorate

    async def aclose(self):
        # limits owns a bridge over the redis-py connection.
        bridge = getattr(self.storage, "bridge", None)
        if bridge is not None:
            closed = set()
            for attr in ("storage", "storage_readonly"):
                client = getattr(bridge, attr, None)
                if (
                    client is not None
                    and id(client) not in closed
                    and hasattr(client, "aclose")
                ):
                    await client.aclose()
                    closed.add(id(client))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()
