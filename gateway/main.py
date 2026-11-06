"""API Gateway entry point."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
import redis.asyncio as aioredis
from fastapi.responses import JSONResponse
from sqlalchemy import text

from common.broker import get_connection, setup_topology
from common.db import get_engine, init_db
from common.config import get_settings
from gateway.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("keyframe.gateway")


CHECK_TIMEOUT_SEC = 3


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Runs once at startup (before `yield`) and once at shutdown (after it)."""
    logger.info("Starting KeyFrame-AI API Gateway")

    # Create `jobs` table.
    await init_db()
    logger.info("Database tables are ready")

    # Declare the exchange, stage queues and dead-letter queue.
    connection = await get_connection()
    try:
        channel = await connection.channel()
        await setup_topology(channel)
    finally:
        await connection.close()
    logger.info("RabbitMQ queues are ready")

    yield

    await get_engine().dispose()
    logger.info("Gateway shut down")


app = FastAPI(
    title="KeyFrame-AI API Gateway",
    description="Distributed Multimodal Video Note-Taking Agent API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)

async def check_postgres() -> None:
    async with get_engine().connect() as conn:
        await conn.execute(text("SELECT 1"))


async def check_redis() -> None:
    client = aioredis.from_url(get_settings().redis_url)
    try:
        await client.ping()
    finally:
        await client.aclose()


async def check_rabbitmq() -> None:
    connection = await get_connection()
    await connection.close()


async def run_check(name: str, check) -> tuple[str, str]:
    """Runs one check with a timeout"""
    try:
        await asyncio.wait_for(check(), timeout=CHECK_TIMEOUT_SEC)
        return name, "ok"
    except Exception as exc:
        logger.warning("%s health check failed: %r", name, exc)
        return name, "fail"


@app.get("/healthz")
async def healthz():
    results = await asyncio.gather(
        run_check("postgres", check_postgres),
        run_check("redis", check_redis),
        run_check("rabbitmq", check_rabbitmq),
    )
    checks = dict(results)
    healthy = all(value == "ok" for value in checks.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "healthy" if healthy else "unhealthy", **checks},
    )