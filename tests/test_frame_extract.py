"""Tests for Stage 3: direct stream frame extraction, graceful degradation, and warning tracking."""

import os
import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.stages.frame_extract import process_frame_extract_stage

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
async def test_frame_extract_stage_success(tmp_path):
    """Verify all flagged frames are extracted and saved, advancing job to PUBLISHING."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="vid_frame_1",
            youtube_url="https://www.youtube.com/watch?v=vid_frame_1",
            notion_token="secret_key",
            status=JobStatus.EXTRACTING_FRAMES.value,
            llm_summary={
                "overview": "Overview text",
                "key_moments": [
                    {"timestamp_sec": 30, "caption": "Slide 1 explanation"},
                    {"timestamp_sec": 90, "caption": "Slide 2 explanation"},
                ],
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
        )

        mock_stream_url_fn = lambda url: "https://googlevideo.com/videoplayback/test_stream"
        mock_capture_fn = lambda url, ts, out: True
        mock_publish = AsyncMock()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("worker.stages.frame_extract.publish_stage_task", mock_publish)
            success = await process_frame_extract_stage(
                task=task,
                session=db_session,
                stream_url_fn=mock_stream_url_fn,
                capture_fn=mock_capture_fn,
                storage_dir=str(tmp_path),
            )

        assert success is True

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.PUBLISHING.value
        assert updated_job.progress == 85
        assert len(updated_job.extracted_frames) == 2
        assert len(updated_job.warnings) == 0
        assert updated_job.extracted_frames[0]["timestamp_sec"] == 30
        assert updated_job.extracted_frames[1]["timestamp_sec"] == 90

        mock_publish.assert_called_once()
        published_task: StageMessage = mock_publish.call_args[0][0]
        assert published_task.stage == "publish"


@pytest.mark.asyncio
async def test_frame_extract_stage_graceful_degradation(tmp_path):
    """Verify partial extraction failure records warning but continues to publish per Section 6."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="vid_frame_partial",
            youtube_url="https://www.youtube.com/watch?v=vid_frame_partial",
            notion_token="secret_key",
            status=JobStatus.EXTRACTING_FRAMES.value,
            llm_summary={
                "overview": "Overview text",
                "key_moments": [
                    {"timestamp_sec": 45, "caption": "Diagram A"},
                    {"timestamp_sec": 120, "caption": "Diagram B (fails to seek)"},
                ],
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
        )

        mock_stream_url_fn = lambda url: "https://googlevideo.com/videoplayback/test_stream"
        # First timestamp succeeds, second fails
        mock_capture_fn = lambda url, ts, out: ts == 45
        mock_publish = AsyncMock()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("worker.stages.frame_extract.publish_stage_task", mock_publish)
            success = await process_frame_extract_stage(
                task=task,
                session=db_session,
                stream_url_fn=mock_stream_url_fn,
                capture_fn=mock_capture_fn,
                storage_dir=str(tmp_path),
            )

        assert success is True

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        # Job is NOT failed; advances to publishing
        assert updated_job.status == JobStatus.PUBLISHING.value
        # Succeeded frame is kept
        assert len(updated_job.extracted_frames) == 1
        assert updated_job.extracted_frames[0]["timestamp_sec"] == 45
        # Warning recorded for failed frame
        assert len(updated_job.warnings) == 1
        assert updated_job.warnings[0]["code"] == "FRAME_EXTRACTION_FAILED"
        assert updated_job.warnings[0]["timestamp_sec"] == 120

        mock_publish.assert_called_once()


@pytest.mark.asyncio
async def test_frame_extract_stage_complete_stream_failure(tmp_path):
    """Verify total stream failure logs warnings for all moments and still publishes text summary."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="vid_stream_fail",
            youtube_url="https://www.youtube.com/watch?v=vid_stream_fail",
            notion_token="secret_key",
            status=JobStatus.EXTRACTING_FRAMES.value,
            llm_summary={
                "overview": "Overview text",
                "key_moments": [
                    {"timestamp_sec": 10, "caption": "Slide 1"},
                    {"timestamp_sec": 20, "caption": "Slide 2"},
                ],
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
        )

        # Stream resolution returns None
        mock_stream_url_fn = lambda url: None
        mock_publish = AsyncMock()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("worker.stages.frame_extract.publish_stage_task", mock_publish)
            success = await process_frame_extract_stage(
                task=task,
                session=db_session,
                stream_url_fn=mock_stream_url_fn,
                storage_dir=str(tmp_path),
            )

        assert success is True

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.PUBLISHING.value
        assert len(updated_job.extracted_frames) == 0
        assert len(updated_job.warnings) == 2
        assert updated_job.warnings[0]["code"] == "FRAME_EXTRACTION_FAILED"

        mock_publish.assert_called_once()

