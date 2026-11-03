"""Tests for worker consumer daemon, manual ACK mechanics, backoff retries, and DLQ routing."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.consumer import StageConsumerDaemon
from worker.dlq import calculate_backoff_delay_sec, route_to_dlq

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@asynccontextmanager
async def get_test_db_session():
    """Provides an isolated in-memory SQLite database session for consumer tests."""
    engine = create_async_engine(
        TEST_DB_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_maker() as session:
        yield session

    await engine.dispose()


def test_calculate_backoff_delay_sec():
    """Verify exponential backoff schedule lookup logic."""
    assert calculate_backoff_delay_sec(0) == 10
    assert calculate_backoff_delay_sec(1) == 30
    assert calculate_backoff_delay_sec(2) == 90
    # Values beyond schedule length use the maximum schedule delay
    assert calculate_backoff_delay_sec(5) == 90


@pytest.mark.asyncio
async def test_route_to_dlq():
    """Verify route_to_dlq updates job state to DEAD_LETTERED and publishes to DLQ."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="dlq_vid",
            youtube_url="https://www.youtube.com/watch?v=dlq_vid",
            notion_token="secret_key",
            status=JobStatus.SUMMARIZING.value,
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="summarize",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
            retry_count=5,
        )

        mock_publish = AsyncMock()
        error_info = {"code": "FATAL_ERROR", "message": "Failed repeatedly"}

        with patch("worker.dlq.publish_stage_task", mock_publish):
            await route_to_dlq(task, error=error_info, session=db_session)

        # Check database update
        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.DEAD_LETTERED.value
        assert updated_job.error["code"] == "FATAL_ERROR"
        assert updated_job.retry_count == 5

        # Check DLQ task published
        mock_publish.assert_called_once()
        published_msg: StageMessage = mock_publish.call_args[0][0]
        assert published_msg.stage == "dlq"
        assert published_msg.job_id == job.id


@pytest.mark.asyncio
async def test_consumer_happy_path_acknowledgment():
    """Verify consumer acks message after stage completes successfully."""
    daemon = StageConsumerDaemon(connection=AsyncMock())
    task = StageMessage(
        job_id="test_job_ack",
        stage="transcribe",
        video_id="ack_vid",
        youtube_url="https://www.youtube.com/watch?v=ack_vid",
        notion_token="secret_key",
    )

    mock_incoming_message = AsyncMock()
    mock_incoming_message.body = task.serialize()

    with patch("worker.consumer.get_db_session", get_test_db_session), patch(
        "worker.consumer.process_transcribe_stage", new=AsyncMock(return_value=True)
    ):
        await daemon._handle_incoming_message(mock_incoming_message, "transcribe")

    mock_incoming_message.ack.assert_called_once()


@pytest.mark.asyncio
async def test_consumer_rate_limit_429_requeue():
    """Verify consumer catches HTTP 429, increments retry count, re-queues with delay, and acks original."""
    daemon = StageConsumerDaemon(connection=AsyncMock())
    task = StageMessage(
        job_id="test_job_429",
        stage="summarize",
        video_id="vid_429",
        youtube_url="https://www.youtube.com/watch?v=vid_429",
        notion_token="secret_key",
        retry_count=0,
    )

    mock_incoming_message = AsyncMock()
    mock_incoming_message.body = task.serialize()

    mock_publish = AsyncMock()

    with patch("worker.consumer.get_db_session", get_test_db_session), patch(
        "worker.consumer.process_summarize_stage", side_effect=Exception("HTTP 429 Too Many Requests")
    ), patch(
        "worker.consumer.publish_stage_task", mock_publish
    ):
        await daemon._handle_incoming_message(mock_incoming_message, "summarize")

    # Original message acknowledged
    mock_incoming_message.ack.assert_called_once()

    # Re-published with delay
    mock_publish.assert_called_once()
    requeued_task: StageMessage = mock_publish.call_args[0][0]
    delay_ms = mock_publish.call_args[1].get("delay_ms")

    assert requeued_task.retry_count == 1
    assert delay_ms == 10000  # 10s from schedule

