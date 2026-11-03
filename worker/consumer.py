"""Multi-queue AMQP stage consumer daemon enforcing idempotency, manual acks, and DLQ routing."""

import asyncio
import logging
from typing import Callable, Dict, Optional
import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage
from sqlalchemy.ext.asyncio import AsyncSession

from common.broker import (
    QUEUE_FRAME_EXTRACT,
    QUEUE_PUBLISH,
    QUEUE_SUMMARIZE,
    QUEUE_TRANSCRIBE,
    StageMessage,
    get_connection,
    publish_stage_task,
    setup_topology,
)
from common.config import get_settings
from common.db import get_db_session
from worker.dlq import calculate_backoff_delay_sec, route_to_dlq
from worker.stages.frame_extract import process_frame_extract_stage
from worker.stages.publish import process_publish_stage
from worker.stages.summarize import process_summarize_stage
from worker.stages.transcribe import process_transcribe_stage

logger = logging.getLogger(__name__)


class StageConsumerDaemon:
    """Manages worker consumer lifecycle, queue subscriptions, manual acks, and fault resilience."""

    def __init__(
        self,
        connection: Optional[AbstractConnection] = None,
        rabbitmq_url: Optional[str] = None,
        prefetch_count: int = 1,
    ) -> None:
        self.connection = connection
        self.rabbitmq_url = rabbitmq_url
        self.prefetch_count = prefetch_count
        self.channel: Optional[AbstractChannel] = None
        self._running = False
        self._consumers = []

    async def start(self) -> None:
        """Connects to RabbitMQ, sets QoS prefetch, and starts listening to all stage queues."""
        if not self.connection:
            self.connection = await get_connection(self.rabbitmq_url)

        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        # Ensure topology exchanges and queues are declared
        queues = await setup_topology(self.channel)
        self._running = True

        # Attach consumer callbacks for each stage queue
        stage_queues = [
            (QUEUE_TRANSCRIBE, "transcribe"),
            (QUEUE_SUMMARIZE, "summarize"),
            (QUEUE_FRAME_EXTRACT, "frame_extract"),
            (QUEUE_PUBLISH, "publish"),
        ]

        for queue_name, stage_name in stage_queues:
            queue = queues[queue_name]
            tag = await queue.consume(
                lambda msg, s=stage_name: self._handle_incoming_message(msg, s)
            )
            self._consumers.append(tag)
            logger.info(f"Worker subscribed to '{queue_name}' with prefetch={self.prefetch_count}")

    async def _handle_incoming_message(
        self,
        message: AbstractIncomingMessage,
        expected_stage: str,
    ) -> None:
        """Processes message within an isolated session and acknowledges strictly after DB commit."""
        settings = get_settings()

        try:
            task = StageMessage.deserialize(message.body)
        except Exception as e:
            logger.error(f"Malformed message payload received on {expected_stage}: {e}")
            await message.ack()
            return

        logger.info(f"Claimed task {task.job_id} on stage '{expected_stage}' (attempt {task.retry_count + 1})")

        async with get_db_session() as session:
            try:
                # Dispatch task to the appropriate stage handler
                if expected_stage == "transcribe":
                    await process_transcribe_stage(task, session, connection=self.connection)
                elif expected_stage == "summarize":
                    await process_summarize_stage(task, session, connection=self.connection)
                elif expected_stage == "frame_extract":
                    await process_frame_extract_stage(task, session, connection=self.connection)
                elif expected_stage == "publish":
                    await process_publish_stage(task, session)
                else:
                    logger.error(f"Unhandled stage: {expected_stage}")

                # Task completed and state committed durably in PostgreSQL -> safe to ACK
                await message.ack()
                logger.info(f"Successfully finished and acknowledged task {task.job_id} at stage '{expected_stage}'")

            except Exception as e:
                error_str = str(e).lower()
                is_rate_limit = "429" in error_str or "rate limit" in error_str or "resource_exhausted" in error_str

                if is_rate_limit:
                    # External API HTTP 429: Re-queue with exponential backoff if retry budget remains
                    if task.retry_count < settings.max_retry_attempts:
                        delay_sec = calculate_backoff_delay_sec(task.retry_count)
                        task.retry_count += 1
                        logger.warning(
                            f"HTTP 429 hit for job {task.job_id} on stage '{expected_stage}'. "
                            f"Re-queuing with {delay_sec}s backoff (Attempt {task.retry_count}/{settings.max_retry_attempts})."
                        )
                        # Re-publish delayed copy and ack the original
                        await publish_stage_task(
                            task,
                            connection=self.connection,
                            delay_ms=delay_sec * 1000,
                        )
                        await message.ack()
                        return
                    else:
                        logger.error(f"Retry budget exhausted for job {task.job_id} on HTTP 429. Routing to DLQ.")
                        await route_to_dlq(
                            task,
                            error={"code": "RATE_LIMIT_EXHAUSTED", "message": str(e)},
                            session=session,
                            connection=self.connection,
                        )
                        await message.ack()
                        return

                # Non rate limit exception: Check retry budget
                task.retry_count += 1
                if task.retry_count >= settings.max_retry_attempts:
                    logger.error(f"Job {task.job_id} exceeded maximum retry budget of {settings.max_retry_attempts}. Routing to DLQ.")
                    await route_to_dlq(
                        task,
                        error={"code": "RETRY_BUDGET_EXHAUSTED", "message": str(e)},
                        session=session,
                        connection=self.connection,
                    )
                    await message.ack()
                else:
                    # Non-fatal error with retries remaining: schedule backoff
                    delay_sec = calculate_backoff_delay_sec(task.retry_count)
                    logger.warning(f"Error on stage '{expected_stage}' for job {task.job_id}. Retrying in {delay_sec}s: {e}")
                    await publish_stage_task(
                        task,
                        connection=self.connection,
                        delay_ms=delay_sec * 1000,
                    )
                    await message.ack()

    async def stop(self) -> None:
        """Gracefully shuts down consumers and connection."""
        self._running = False
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()

