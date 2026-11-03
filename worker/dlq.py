"""Dead Letter Queue routing and retry backoff mechanics."""

import logging
from typing import Any, Dict, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from aio_pika.abc import AbstractConnection

from common.broker import QUEUE_DLQ, ROUTING_KEY_DLQ, StageMessage, publish_stage_task
from common.config import get_settings
from common.models import Job, JobStatus

logger = logging.getLogger(__name__)


def calculate_backoff_delay_sec(retry_count: int) -> int:
    """Calculates backoff delay in seconds based on configured schedule."""
    schedule = get_settings().retry_backoff_schedule
    if not schedule:
        return 10
    idx = min(retry_count, len(schedule) - 1)
    return schedule[idx]


async def route_to_dlq(
    task: StageMessage,
    error: Dict[str, Any],
    session: AsyncSession,
    connection: Optional[AbstractConnection] = None,
) -> None:
    """Routes an unrecoverable task to the Dead Letter Queue and marks the job DEAD_LETTERED."""
    logger.error(f"Routing job {task.job_id} at stage '{task.stage}' to DLQ. Error: {error}")

    # Update job state in database
    result = await session.execute(select(Job).where(Job.id == task.job_id))
    job = result.scalar_one_or_none()
    if job:
        job.transition_to(JobStatus.DEAD_LETTERED, error=error)
        job.retry_count = task.retry_count
        await session.commit()

    # Publish to DLQ
    dlq_message = StageMessage(
        job_id=task.job_id,
        stage="dlq",
        video_id=task.video_id,
        youtube_url=task.youtube_url,
        notion_token=task.notion_token,
        notion_parent_id=task.notion_parent_id,
        retry_count=task.retry_count,
        last_error=error,
    )
    await publish_stage_task(dlq_message, connection=connection)

