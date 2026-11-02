"""Distributed token bucket rate limiter backed by Redis for multi-worker coordination."""

import asyncio
import math
import time
from typing import Any, Optional, Tuple
import redis.asyncio as aioredis
from common.config import get_settings

# Atomic token bucket Lua script executed on Redis server
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens or not last_updated then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + delta * refill_rate)
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    local ttl = math.ceil(capacity / refill_rate) * 2
    redis.call('EXPIRE', key, math.max(ttl, 60))
    return {1, tokens, 0}
else
    local needed = requested - tokens
    local wait_time = needed / refill_rate
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    return {0, tokens, wait_time}
end
"""


class InMemoryTokenBucket:
    """Fallback in memory token bucket for standalone testing without Redis."""

    def __init__(self, capacity: int, refill_rate_per_sec: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate_per_sec
        self.tokens = float(capacity)
        self.last_updated = time.time()

    def try_acquire(self, requested: int = 1) -> Tuple[bool, float, float]:
        now = time.time()
        delta = max(0.0, now - self.last_updated)
        self.tokens = min(float(self.capacity), self.tokens + delta * self.refill_rate)
        self.last_updated = now

        if self.tokens >= requested:
            self.tokens -= requested
            return True, self.tokens, 0.0
        else:
            needed = requested - self.tokens
            wait_time = needed / self.refill_rate
            return False, self.tokens, wait_time


class DistributedRateLimiter:
    """Coordinates global external API rate limits across distributed worker instances."""

    def __init__(
        self,
        redis_client: Optional[aioredis.Redis] = None,
        redis_url: Optional[str] = None,
        enable_redis: bool = True,
    ) -> None:
        self._redis_client = redis_client
        self._redis_url = redis_url or get_settings().redis_url
        self._enable_redis = enable_redis
        self._redis_available: Optional[bool] = None
        self._last_redis_check: float = 0.0
        self._fallback_buckets = {}

    async def _get_client(self) -> Optional[aioredis.Redis]:
        if not self._enable_redis:
            return None
        if self._redis_client is not None:
            return self._redis_client
        if self._redis_available is False:
            # Recheck Redis only after 30 seconds cooldown to avoid blocking timeouts
            if time.time() - self._last_redis_check < 30.0:
                return None

        self._last_redis_check = time.time()
        try:
            client = aioredis.from_url(
                self._redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=0.2,
            )
            await asyncio.wait_for(client.ping(), timeout=0.2)
            self._redis_client = client
            self._redis_available = True
            return self._redis_client
        except Exception:
            self._redis_available = False
            return None

    async def acquire(
        self,
        key: str,
        max_rate: int,
        time_window_sec: float = 60.0,
        requested: int = 1,
        block: bool = True,
        timeout: float = 30.0,
    ) -> bool:
        """Acquires execution permission under the specified rate limit.
        
        Args:
            key: Rate limit identifier (e.g. 'llm_gemini_global').
            max_rate: Maximum number of operations allowed per window.
            time_window_sec: Duration of the window in seconds (default 60 for RPM).
            requested: Tokens to consume (default 1).
            block: If True, sleeps until tokens regenerate or timeout expires.
            timeout: Maximum seconds to wait if blocked.
            
        Returns:
            True if permission acquired, False if timeout exceeded or limit hit.
        """
        refill_rate = max_rate / time_window_sec
        deadline = time.time() + timeout
        client = await self._get_client()

        while True:
            now = time.time()
            if client is not None:
                try:
                    full_key = f"rate_limit:{key}"
                    res = await client.eval(
                        TOKEN_BUCKET_LUA,
                        1,
                        full_key,
                        max_rate,
                        refill_rate,
                        now,
                        requested,
                    )
                    allowed = bool(res[0])
                    wait_time = float(res[2])
                except Exception:
                    # On Redis failure during command, fallback gracefully
                    allowed, _, wait_time = self._fallback_acquire(key, max_rate, refill_rate, requested)
            else:
                allowed, _, wait_time = self._fallback_acquire(key, max_rate, refill_rate, requested)

            if allowed:
                return True

            if not block:
                return False

            remaining_timeout = deadline - time.time()
            if remaining_timeout <= 0:
                return False

            sleep_duration = min(wait_time, remaining_timeout)
            await asyncio.sleep(max(0.01, sleep_duration))

    def _fallback_acquire(
        self,
        key: str,
        capacity: int,
        refill_rate: float,
        requested: int,
    ) -> Tuple[bool, float, float]:
        if key not in self._fallback_buckets:
            self._fallback_buckets[key] = InMemoryTokenBucket(capacity, refill_rate)
        bucket = self._fallback_buckets[key]
        return bucket.try_acquire(requested)

    async def close(self) -> None:
        """Closes the underlying Redis connection pool if initialized."""
        if self._redis_client is not None:
            await self._redis_client.close()
            self._redis_client = None
"""Distributed token bucket rate limiter backed by Redis for multi-worker coordination."""

import asyncio
import math
import time
from typing import Any, Optional, Tuple
import redis.asyncio as aioredis
from common.config import get_settings

# Atomic token bucket Lua script executed on Redis server
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens or not last_updated then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + delta * refill_rate)
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    local ttl = math.ceil(capacity / refill_rate) * 2
    redis.call('EXPIRE', key, math.max(ttl, 60))
    return {1, tokens, 0}
else
    local needed = requested - tokens
    local wait_time = needed / refill_rate
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    return {0, tokens, wait_time}
end
"""


class InMemoryTokenBucket:
    """Fallback in memory token bucket for standalone testing without Redis."""

    def __init__(self, capacity: int, refill_rate_per_sec: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate_per_sec
        self.tokens = float(capacity)
        self.last_updated = time.time()

    def try_acquire(self, requested: int = 1) -> Tuple[bool, float, float]:
        now = time.time()
        delta = max(0.0, now - self.last_updated)
        self.tokens = min(float(self.capacity), self.tokens + delta * self.refill_rate)
        self.last_updated = now

        if self.tokens >= requested:
            self.tokens -= requested
            return True, self.tokens, 0.0
        else:
            needed = requested - self.tokens
            wait_time = needed / self.refill_rate
            return False, self.tokens, wait_time


class DistributedRateLimiter:
    """Coordinates global external API rate limits across distributed worker instances."""

    def __init__(
        self,
        redis_client: Optional[aioredis.Redis] = None,
        redis_url: Optional[str] = None,
        enable_redis: bool = True,
    ) -> None:
        self._redis_client = redis_client
        self._redis_url = redis_url or get_settings().redis_url
        self._enable_redis = enable_redis
        self._redis_available: Optional[bool] = None
        self._last_redis_check: float = 0.0
        self._fallback_buckets = {}

    async def _get_client(self) -> Optional[aioredis.Redis]:
        if not self._enable_redis:
            return None
        if self._redis_client is not None:
            return self._redis_client
        if self._redis_available is False:
            if time.time() - self._last_redis_check < 30.0:
                return None

        self._last_redis_check = time.time()
        try:
            client = aioredis.from_url(
                self._redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=0.2,
            )
            await asyncio.wait_for(client.ping(), timeout=0.2)
            self._redis_client = client
            self._redis_available = True
            return self._redis_client
        except Exception:
            self._redis_available = False
            return None

    async def acquire(
        self,
        key: str,
        max_rate: int,
        time_window_sec: float = 60.0,
        requested: int = 1,
        block: bool = True,
        timeout: float = 30.0,
    ) -> bool:
        """Acquires execution permission under the specified rate limit."""
        refill_rate = max_rate / time_window_sec
        deadline = time.time() + timeout
        client = await self._get_client()

        while True:
            now = time.time()
            if client is not None:
                try:
                    full_key = f"rate_limit:{key}"
                    res = await client.eval(
                        TOKEN_BUCKET_LUA,
                        1,
                        full_key,
                        max_rate,
                        refill_rate,
                        now,
                        requested,
                    )
                    allowed = bool(res[0])
                    wait_time = float(res[2])
                except Exception:
                    allowed, _, wait_time = self._fallback_acquire(key, max_rate, refill_rate, requested)
            else:
                allowed, _, wait_time = self._fallback_acquire(key, max_rate, refill_rate, requested)

            if allowed:
                return True

            if not block:
                return False

            remaining_timeout = deadline - time.time()
            if remaining_timeout <= 0:
                return False

            sleep_duration = min(wait_time, remaining_timeout)
            await asyncio.sleep(max(0.01, sleep_duration))

    def _fallback_acquire(
        self,
        key: str,
        capacity: int,
        refill_rate: float,
        requested: int,
    ) -> Tuple[bool, float, float]:
        if key not in self._fallback_buckets:
            self._fallback_buckets[key] = InMemoryTokenBucket(capacity, refill_rate)
        bucket = self._fallback_buckets[key]
        return bucket.try_acquire(requested)

    async def close(self) -> None:
        """Closes the underlying Redis connection pool if initialized."""
        if self._redis_client is not None:
            await self._redis_client.close()
            self._redis_client = None

