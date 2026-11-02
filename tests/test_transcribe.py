"""Tests for Stage 1: transcript extraction, metadata retrieval, and Constraint A enforcement."""

import pytest
from unittest.mock import AsyncMock, patch
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from youtube_transcript_api import NoTranscriptFound

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.stages.transcribe import process_transcribe_stage

from contextlib import asynccontextmanager

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@asynccontextmanager
async def get_test_db_session():
    """Provides an isolated in-memory SQLite database session for stage tests."""
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
async def test_transcribe_stage_valid_video():
    """Verify valid video (< 20m) extracts transcript, updates DB, and publishes to summarize queue."""
    async with get_test_db_session() as db_session:
        # Create initial job
        job = Job(
            video_id="valid_vid_123",
            youtube_url="https://www.youtube.com/watch?v=valid_vid_123",
            notion_token="secret_notion_key",
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="transcribe",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
        )

        mock_transcript = [
            {"text": "Intro to Distributed Systems", "start": 0.0, "duration": 5.0},
            {"text": "Key architecture principles", "start": 5.0, "duration": 10.0},
        ]

        mock_publish = AsyncMock()

        with patch(
            "worker.stages.transcribe.fetch_transcript_and_metadata",
            new=AsyncMock(return_value=(mock_transcript, 600, "Distributed Systems Lecture")),
        ), patch(
            "worker.stages.transcribe.publish_stage_task",
            new=mock_publish,
        ):
            success = await process_transcribe_stage(task, db_session)

        assert success is True

        # Verify database updates
        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.SUMMARIZING.value
        assert updated_job.duration_sec == 600
        assert updated_job.video_title == "Distributed Systems Lecture"
        assert len(updated_job.raw_transcript) == 2
        assert updated_job.progress == 50

        # Verify next stage message was published
        mock_publish.assert_called_once()
        published_task: StageMessage = mock_publish.call_args[0][0]
        assert published_task.stage == "summarize"
        assert published_task.job_id == job.id


@pytest.mark.asyncio
async def test_transcribe_stage_constraint_a_rejection():
    """Verify Constraint A: video > 20 minutes (1200s) is deterministically rejected with VIDEO_TOO_LONG."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="long_vid_456",
            youtube_url="https://www.youtube.com/watch?v=long_vid_456",
            notion_token="secret_notion_key",
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="transcribe",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
        )

        mock_transcript = [{"text": "Long video content", "start": 0.0, "duration": 1500.0}]
        mock_publish = AsyncMock()

        with patch(
            "worker.stages.transcribe.fetch_transcript_and_metadata",
            new=AsyncMock(return_value=(mock_transcript, 1500, "Two Hour Tutorial")),
        ), patch(
            "worker.stages.transcribe.publish_stage_task",
            new=mock_publish,
        ):
            success = await process_transcribe_stage(task, db_session)

        assert success is False

        # Verify status is FAILED and Constraint A error is set
        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.FAILED.value
        assert updated_job.error is not None
        assert updated_job.error["code"] == "VIDEO_TOO_LONG"
        assert "20 minutes" in updated_job.error["message"]

        # Verify stage 2 is NOT enqueued
        mock_publish.assert_not_called()


@pytest.mark.asyncio
async def test_transcribe_stage_missing_transcript():
    """Verify missing transcripts terminate cleanly with TRANSCRIPT_NOT_FOUND error."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="no_sub_vid",
            youtube_url="https://www.youtube.com/watch?v=no_sub_vid",
            notion_token="secret_notion_key",
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="transcribe",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
        )

        mock_publish = AsyncMock()

        with patch(
            "worker.stages.transcribe.fetch_transcript_and_metadata",
            side_effect=NoTranscriptFound("no_sub_vid", [], []),
        ), patch(
            "worker.stages.transcribe.publish_stage_task",
            new=mock_publish,
        ):
            success = await process_transcribe_stage(task, db_session)

        assert success is False

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.FAILED.value
        assert updated_job.error["code"] == "TRANSCRIPT_NOT_FOUND"
        mock_publish.assert_not_called()
