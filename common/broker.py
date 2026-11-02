"""RabbitMQ broker connection management, stage queue topologies, and message schemas."""

import json
from typing import Any, Dict, Optional
import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractQueue,
    DeliveryMode,
)
from pydantic import BaseModel, Field

from common.config import get_settings

EXCHANGE_NAME = "notes.direct"
DLX_EXCHANGE_NAME = "notes.dlx"

QUEUE_TRANSCRIBE = "jobs.transcribe"
QUEUE_SUMMARIZE = "jobs.summarize"
QUEUE_FRAME_EXTRACT = "jobs.frame_extract"
QUEUE_PUBLISH = "jobs.publish"
QUEUE_DLQ = "jobs.dlq"

ROUTING_KEY_TRANSCRIBE = "transcribe"
ROUTING_KEY_SUMMARIZE = "summarize"
ROUTING_KEY_FRAME_EXTRACT = "frame_extract"
ROUTING_KEY_PUBLISH = "publish"
ROUTING_KEY_DLQ = "dlq"

STAGE_ROUTING_KEYS = {
    "transcribe": ROUTING_KEY_TRANSCRIBE,
    "summarize": ROUTING_KEY_SUMMARIZE,
    "frame_extract": ROUTING_KEY_FRAME_EXTRACT,
    "publish": ROUTING_KEY_PUBLISH,
    "dlq": ROUTING_KEY_DLQ,
}


class StageMessage(BaseModel):
    """Uniform message envelope passed through RabbitMQ stage queues."""

    job_id: str
    stage: str
    video_id: str
    youtube_url: str
    notion_token: str
    notion_parent_id: Optional[str] = None
    retry_count: int = Field(default=0, ge=0)
    last_error: Optional[Dict[str, Any]] = None

    def serialize(self) -> bytes:
        """Serializes message model to UTF-8 JSON bytes."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def deserialize(cls, payload: bytes) -> "StageMessage":
        """Deserializes message bytes into validated StageMessage."""
        data = json.loads(payload.decode("utf-8"))
        return cls(**data)


async def get_connection(rabbitmq_url: Optional[str] = None) -> AbstractConnection:
    """Creates an async connection to the RabbitMQ broker."""
    url = rabbitmq_url or get_settings().rabbitmq_url
    return await aio_pika.connect_robust(url)


async def setup_topology(channel: AbstractChannel) -> Dict[str, AbstractQueue]:
    """Declares durable direct exchanges and stage queues with dead letter bindings."""
    # Declare primary and dead letter exchanges
    direct_exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    dlx_exchange = await channel.declare_exchange(
        DLX_EXCHANGE_NAME,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )

    # Declare Dead Letter Queue
    dlq = await channel.declare_queue(
        QUEUE_DLQ,
        durable=True,
    )
    await dlq.bind(dlx_exchange, routing_key=ROUTING_KEY_DLQ)

    # Queue arguments routing dropped or failed tasks to DLX
    queue_args = {
        "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
        "x-dead-letter-routing-key": ROUTING_KEY_DLQ,
    }

    # Declare and bind each stage queue
    queues: Dict[str, AbstractQueue] = {QUEUE_DLQ: dlq}
    stage_mappings = [
        (QUEUE_TRANSCRIBE, ROUTING_KEY_TRANSCRIBE),
        (QUEUE_SUMMARIZE, ROUTING_KEY_SUMMARIZE),
        (QUEUE_FRAME_EXTRACT, ROUTING_KEY_FRAME_EXTRACT),
        (QUEUE_PUBLISH, ROUTING_KEY_PUBLISH),
    ]

    for queue_name, routing_key in stage_mappings:
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments=queue_args,
        )
        await queue.bind(direct_exchange, routing_key=routing_key)
        queues[queue_name] = queue

    return queues


async def publish_stage_task(
    message: StageMessage,
    connection: Optional[AbstractConnection] = None,
    rabbitmq_url: Optional[str] = None,
    delay_ms: Optional[int] = None,
) -> None:
    """Publishes a durable stage task message to RabbitMQ with persistent delivery."""
    routing_key = STAGE_ROUTING_KEYS.get(message.stage)
    if not routing_key:
        raise ValueError(f"Unknown pipeline stage: {message.stage}")

    headers: Dict[str, Any] = {}
    if delay_ms is not None and delay_ms > 0:
        headers["x-delay"] = delay_ms

    msg = aio_pika.Message(
        body=message.serialize(),
        delivery_mode=DeliveryMode.PERSISTENT,
        content_type="application/json",
        headers=headers,
    )

    should_close = False
    conn = connection
    if conn is None:
        conn = await get_connection(rabbitmq_url)
        should_close = True

    try:
        channel = await conn.channel()
        exchange = await channel.get_exchange(EXCHANGE_NAME)
        await exchange.publish(msg, routing_key=routing_key)
    finally:
        if should_close:
            await conn.close()

