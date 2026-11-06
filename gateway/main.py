"""API Gateway entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from common.broker import get_connection, setup_topology
from common.db import get_engine, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("keyframe.gateway")


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


@app.get("/healthz")
async def healthz():
    """Basic testing check."""
    return {"status": "ok"}