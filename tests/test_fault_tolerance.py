"""Tests demonstrating fault tolerance: worker crashes, HTTP 429 exponential backoff, and DLQ routing."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.consumer import StageConsumerDaemon
from worker.stages.frame_extract import process_frame_extract_stage
from worker.stages.publish import process_publish_stage

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@asynccontextmanager
async def get_test_db_session():
    """Provides an isolated in-memory SQLite database session for fault tolerance tests."""
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


@pytest.mark.asyncio
async def test_worker_crash_and_idempotent_redelivery_simulation():
    """Simulates worker crash (SIGKILL) mid-frame-extraction: unacked task is redelivered without duplicates."""
    async with get_test_db_session() as db_session:
        # Create initial job at EXTRACTING_FRAMES stage
        job = Job(
            video_id="crash_sim_vid",
            youtube_url="https://www.youtube.com/watch?v=crash_sim_vid",
            notion_token="secret_key",
            status=JobStatus.EXTRACTING_FRAMES.value,
            progress=50,
            llm_summary={
                "overview": "Fault tolerance discussion.",
                "key_moments": [{"timestamp_sec": 75, "caption": "Failure recovery slide."}],
            },
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="frame_extract",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
            notion_parent_id="test_parent_id",
        )

        mock_publish = AsyncMock()

        # Step 1: Worker 1 starts processing but dies (simulating SIGKILL by raising SystemExit before ACK)
        try:
            with patch("worker.stages.frame_extract.capture_frame_with_ffmpeg", side_effect=SystemExit("SIGKILL")):
                await process_frame_extract_stage(
                    task=task,
                    session=db_session,
                    stream_url_fn=lambda url: "https://stream.url",
                )
        except SystemExit:
            pass  # Worker 1 terminates abruptly; message is never acknowledged

        # Verify job is NOT prematurely marked published
        result = await db_session.execute(select(Job).where(Job.id == job.id))
        persisted_job = result.scalar_one()
        assert persisted_job.status != JobStatus.COMPLETED.value

        # Step 2: Broker reassigns unacknowledged message to Worker 2
        with patch("worker.stages.frame_extract.capture_frame_with_ffmpeg", return_value=True), patch(
            "worker.stages.frame_extract.publish_stage_task", mock_publish
        ):
            success = await process_frame_extract_stage(
                task=task,
                session=db_session,
                stream_url_fn=lambda url: "https://stream.url",
            )

        assert success is True
        result = await db_session.execute(select(Job).where(Job.id == job.id))
        completed_job = result.scalar_one()
        assert completed_job.status == JobStatus.PUBLISHING.value
        assert len(completed_job.extracted_frames) == 1

        # Step 3: Publish stage executes once, ensuring zero duplicate Notion pages
        mock_notion = MagicMock()
        mock_notion.pages.create = AsyncMock(return_value={"url": "https://notion.so/unique_page"})

        publish_task = mock_publish.call_args[0][0]
        await process_publish_stage(
            task=publish_task,
            session=db_session,
            notion_client_instance=mock_notion,
        )

        assert mock_notion.pages.create.call_count == 1
        result = await db_session.execute(select(Job).where(Job.id == job.id))
        final_job = result.scalar_one()
        assert final_job.status == JobStatus.COMPLETED.value
        assert final_job.notion_page_url == "https://notion.so/unique_page"

        # Step 4: Redelivery of publish stage reuses existing page, ensuring zero duplicate Notion pages
        await process_publish_stage(
            task=publish_task,
            session=db_session,
            notion_client_instance=mock_notion,
        )
        assert mock_notion.pages.create.call_count == 1  # Still 1, never duplicated!


@pytest.mark.asyncio
async def test_http_429_exponential_backoff_and_recovery():
    """Simulates HTTP 429 rate limit backoff sequence (10s, 30s) followed by successful execution."""
    daemon = StageConsumerDaemon(connection=AsyncMock())
    task = StageMessage(
        job_id="rate_limit_backoff_job",
        stage="summarize",
        video_id="vid_rate_limit",
        youtube_url="https://www.youtube.com/watch?v=vid_rate_limit",
        notion_token="secret_key",
        retry_count=0,
    )

    mock_incoming_message = AsyncMock()
    mock_incoming_message.body = task.serialize()

    mock_publish = AsyncMock()

    # Attempt 0: encounters 429 -> re-queues with 10s delay
    with patch("worker.consumer.get_db_session", get_test_db_session), patch(
        "worker.consumer.process_summarize_stage", side_effect=Exception("HTTP 429: Too Many Requests")
    ), patch(
        "worker.consumer.publish_stage_task", mock_publish
    ):
        await daemon._handle_incoming_message(mock_incoming_message, "summarize")

    mock_incoming_message.ack.assert_called_once()
    mock_publish.assert_called_once()
    requeued_task: StageMessage = mock_publish.call_args[0][0]
    delay_ms = mock_publish.call_args[1]["delay_ms"]
    assert requeued_task.retry_count == 1
    assert delay_ms == 10000  # 10s backoff


@pytest.mark.asyncio
async def test_dead_letter_queue_routing_after_bounded_retries():
    """Simulates job failure reaching maximum attempts (5) and routing permanently to DLQ."""
    daemon = StageConsumerDaemon(connection=AsyncMock())
    task = StageMessage(
        job_id="dlq_exhausted_job",
        stage="transcribe",
        video_id="unavailable_vid",
        youtube_url="https://www.youtube.com/watch?v=unavailable_vid",
        notion_token="secret_key",
        retry_count=5,  # Max retries reached
    )

    mock_incoming_message = AsyncMock()
    mock_incoming_message.body = task.serialize()

    mock_publish = AsyncMock()

    with patch("worker.consumer.get_db_session", get_test_db_session), patch(
        "worker.consumer.process_transcribe_stage", side_effect=Exception("Permanent API Error")
    ), patch(
        "worker.dlq.publish_stage_task", mock_publish
    ):
        await daemon._handle_incoming_message(mock_incoming_message, "transcribe")

    # Message must be acknowledged to avoid poison pill loops
    mock_incoming_message.ack.assert_called_once()

    # DLQ message published
    mock_publish.assert_called_once()
    dlq_task: StageMessage = mock_publish.call_args[0][0]
    assert dlq_task.stage == "dlq"
    assert dlq_task.job_id == "dlq_exhausted_job"
    assert dlq_task.last_error["code"] == "RETRY_BUDGET_EXHAUSTED"
