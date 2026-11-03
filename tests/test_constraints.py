"""Automated grading tests for Hard Constraint A (Video Length) and Hard Constraint B (Real Frames & Captions)."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from pydantic import ValidationError

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.llm.base import SlideKeyMoment
from worker.stages.publish import construct_page_blocks
from worker.stages.transcribe import process_transcribe_stage

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@asynccontextmanager
async def get_test_db_session():
    """Provides an isolated in-memory SQLite database session for constraint tests."""
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


@pytest.mark.parametrize(
    "duration_sec,should_succeed",
    [
        (300, True),    # 5 minutes -> Valid
        (600, True),    # 10 minutes -> Valid
        (1200, True),   # 20 minutes exactly -> Valid baseline
        (1201, False),  # 20 minutes 1 second -> Exceeds cap
        (1800, False),  # 30 minutes -> Exceeds cap
        (3600, False),  # 1 hour -> Exceeds cap
    ],
)
@pytest.mark.asyncio
async def test_constraint_a_video_length_deterministic_rejection(duration_sec: int, should_succeed: bool):
    """Enforces Constraint A: Videos up to 20m (1200s) proceed; videos > 20m are deterministically rejected."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id=f"test_vid_{duration_sec}",
            youtube_url=f"https://www.youtube.com/watch?v=test_vid_{duration_sec}",
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

        mock_transcript = [{"text": "Lecture audio content", "start": 0.0, "duration": float(duration_sec)}]
        mock_publish = AsyncMock()

        with patch(
            "worker.stages.transcribe.fetch_transcript_and_metadata",
            new=AsyncMock(return_value=(mock_transcript, duration_sec, "Lecture Title")),
        ), patch(
            "worker.stages.transcribe.publish_stage_task",
            new=mock_publish,
        ):
            success = await process_transcribe_stage(task, db_session)

        assert success is should_succeed

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        persisted_job = result.scalar_one()

        if should_succeed:
            assert persisted_job.status == JobStatus.SUMMARIZING.value
            assert persisted_job.duration_sec == duration_sec
            mock_publish.assert_called_once()
        else:
            assert persisted_job.status == JobStatus.FAILED.value
            assert persisted_job.error is not None
            assert persisted_job.error["code"] == "VIDEO_TOO_LONG"
            assert "20 minutes" in persisted_job.error["message"]
            mock_publish.assert_not_called()


def test_constraint_b_all_embedded_frames_must_have_captions():
    """Enforces Constraint B: Every frame embedded into Notion MUST be paired with an LLM explanatory caption."""
    # 1. SlideKeyMoment must reject empty or whitespace captions
    with pytest.raises(ValidationError):
        SlideKeyMoment(timestamp_sec=42, caption="")

    with pytest.raises(ValidationError):
        SlideKeyMoment(timestamp_sec=42, caption="   ")

    # 2. Check blocks constructed for Notion page
    valid_job = Job(
        video_id="constraint_b_vid",
        youtube_url="https://www.youtube.com/watch?v=constraint_b_vid",
        notion_token="secret_key",
        duration_sec=500,
        llm_summary={
            "overview": "Overview text.",
            "key_moments": [
                {"timestamp_sec": 60, "caption": "Slide 1 showing cluster topology."},
                {"timestamp_sec": 180, "caption": "Code snippet showing async task definition."},
            ],
        },
        extracted_frames=[
            {"timestamp_sec": 60, "image_path": "/frames/60.jpg", "caption": "Slide 1 showing cluster topology."},
            {"timestamp_sec": 180, "image_path": "/frames/180.jpg", "caption": "Code snippet showing async task definition."},
        ],
    )

    blocks = construct_page_blocks(valid_job)
    image_blocks = [b for b in blocks if b["type"] == "image"]
    assert len(image_blocks) == 2

    # Every image block must contain a non-empty caption matching the LLM explanation
    for idx, img_block in enumerate(image_blocks):
        image_data = img_block["image"]
        assert "caption" in image_data
        assert len(image_data["caption"]) > 0
        caption_text = image_data["caption"][0]["text"]["content"]
        assert len(caption_text.strip()) > 5
        assert valid_job.extracted_frames[idx]["caption"] == caption_text
