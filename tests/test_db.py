"""Tests for database models, state machine transitions, and async session persistence."""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import StaticPool
from common.models import (
    Base,
    Job,
    JobStatus,
    can_transition,
    default_progress_for_status,
)

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


def test_job_status_properties():
    """Verify state machine terminal status properties and transition checks."""
    assert JobStatus.COMPLETED.is_terminal
    assert JobStatus.COMPLETED_WITH_WARNINGS.is_terminal
    assert JobStatus.FAILED.is_terminal
    assert JobStatus.DEAD_LETTERED.is_terminal
    assert not JobStatus.QUEUED.is_terminal
    assert not JobStatus.TRANSCRIBING.is_terminal

    # Valid transitions
    assert can_transition(JobStatus.QUEUED, JobStatus.TRANSCRIBING)
    assert can_transition(JobStatus.TRANSCRIBING, JobStatus.SUMMARIZING)
    assert can_transition(JobStatus.SUMMARIZING, JobStatus.EXTRACTING_FRAMES)
    assert can_transition(JobStatus.EXTRACTING_FRAMES, JobStatus.PUBLISHING)
    assert can_transition(JobStatus.PUBLISHING, JobStatus.COMPLETED)
    assert can_transition(JobStatus.PUBLISHING, JobStatus.COMPLETED_WITH_WARNINGS)
    assert can_transition(JobStatus.TRANSCRIBING, JobStatus.FAILED)

    # Invalid transitions
    assert not can_transition(JobStatus.QUEUED, JobStatus.COMPLETED)
    assert not can_transition(JobStatus.COMPLETED, JobStatus.TRANSCRIBING)


def test_job_model_state_transitions():
    """Verify job entity state mutation and validation logic."""
    job = Job(
        video_id="dQw4w9WgXcQ",
        youtube_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        notion_token="secret_test_token",
    )
    assert job.status == JobStatus.QUEUED.value
    assert job.progress == 0

    # Advance stage
    job.transition_to(JobStatus.TRANSCRIBING)
    assert job.status == JobStatus.TRANSCRIBING.value
    assert job.progress == default_progress_for_status(JobStatus.TRANSCRIBING)

    # Adding non-fatal warning
    job.add_warning("FRAME_EXTRACTION_FAILED", "Seek timed out", timestamp_sec=142)
    assert len(job.warnings) == 1
    assert job.warnings[0]["code"] == "FRAME_EXTRACTION_FAILED"
    assert job.warnings[0]["timestamp_sec"] == 142

    # Illegal jump must raise ValueError
    with pytest.raises(ValueError):
        job.transition_to(JobStatus.COMPLETED)


@pytest.mark.asyncio
async def test_async_database_lifecycle():
    """Verify async database table creation, insertion, query, and update."""
    engine = create_async_engine(
        TEST_DB_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    job_id = None
    # Insert new job
    async with session_maker() as session:
        new_job = Job(
            video_id="test_vid_123",
            youtube_url="https://www.youtube.com/watch?v=test_vid_123",
            notion_token="secret_abc",
            raw_transcript=[{"text": "Hello world", "start": 0.0, "duration": 2.5}],
        )
        session.add(new_job)
        await session.commit()
        job_id = new_job.id

    assert job_id is not None

    # Query and mutate status
    async with session_maker() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        persisted_job = result.scalar_one_or_none()
        assert persisted_job is not None
        assert persisted_job.video_id == "test_vid_123"
        assert persisted_job.status == JobStatus.QUEUED.value
        assert len(persisted_job.raw_transcript) == 1

        persisted_job.transition_to(JobStatus.TRANSCRIBING)
        persisted_job.duration_sec = 600
        persisted_job.video_title = "Async Python Architecture"
        await session.commit()

    # Confirm mutation persisted
    async with session_maker() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        updated_job = result.scalar_one()
        assert updated_job.status == JobStatus.TRANSCRIBING.value
        assert updated_job.duration_sec == 600
        assert updated_job.video_title == "Async Python Architecture"

    await engine.dispose()
