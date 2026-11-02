"""Tests for distributed token bucket rate limiter and fallback mechanisms."""

import asyncio
import time
import pytest
from unittest.mock import AsyncMock
from common.rate_limiter import DistributedRateLimiter, InMemoryTokenBucket


def test_in_memory_token_bucket_basics():
    """Verify in-memory token bucket consumes tokens and calculates needed wait time."""
    bucket = InMemoryTokenBucket(capacity=2, refill_rate_per_sec=1.0)

    # First two acquisitions should succeed immediately
    ok1, rem1, wait1 = bucket.try_acquire(1)
    assert ok1 is True
    assert abs(rem1 - 1.0) < 0.05
    assert wait1 == 0.0

    ok2, rem2, wait2 = bucket.try_acquire(1)
    assert ok2 is True
    assert rem2 < 0.05

    # Third acquisition should be denied with wait time
    ok3, rem3, wait3 = bucket.try_acquire(1)
    assert ok3 is False
    assert wait3 > 0.0


@pytest.mark.asyncio
async def test_distributed_rate_limiter_fallback_acquire():
    """Verify rate limiter allows requests up to limit and blocks/rejects when exhausted."""
    limiter = DistributedRateLimiter(enable_redis=False)

    # 3 requests allowed per 1 second
    assert await limiter.acquire("test_endpoint", max_rate=3, time_window_sec=1.0, block=False) is True
    assert await limiter.acquire("test_endpoint", max_rate=3, time_window_sec=1.0, block=False) is True
    assert await limiter.acquire("test_endpoint", max_rate=3, time_window_sec=1.0, block=False) is True

    # 4th request without blocking must be rejected
    assert await limiter.acquire("test_endpoint", max_rate=3, time_window_sec=1.0, block=False) is False

    # 5th request with short blocking timeout will wait and succeed as tokens refill
    t0 = time.time()
    res = await limiter.acquire("test_endpoint", max_rate=3, time_window_sec=1.0, block=True, timeout=2.0)
    elapsed = time.time() - t0
    assert res is True
    assert elapsed > 0.1


@pytest.mark.asyncio
async def test_distributed_rate_limiter_timeout():
    """Verify acquire returns False when timeout is shorter than wait time."""
    limiter = DistributedRateLimiter(enable_redis=False)

    # 1 request per 10 seconds
    assert await limiter.acquire("slow_endpoint", max_rate=1, time_window_sec=10.0, block=False) is True

    # Second request with only 0.05s timeout must time out
    res = await limiter.acquire("slow_endpoint", max_rate=1, time_window_sec=10.0, block=True, timeout=0.05)
    assert res is False


@pytest.mark.asyncio
async def test_distributed_rate_limiter_redis_eval_call():
    """Verify rate limiter passes correct arguments to Redis Lua script."""
    mock_redis = AsyncMock()
    mock_redis.eval.return_value = [1, 14, 0]

    limiter = DistributedRateLimiter(redis_client=mock_redis)
    result = await limiter.acquire("llm_global", max_rate=15, time_window_sec=60.0, block=False)

    assert result is True
    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[1] == 1  # number of keys
    assert args[2] == "rate_limit:llm_global"
    assert args[3] == 15  # max_rate
    assert args[4] == 15 / 60.0  # refill_rate

