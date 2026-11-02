"""Tests for Stage 2: multimodal summarization, chunking, and Constraint B verification."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.llm.base import SlideKeyMoment, SummarySection, SynthesisResult
from worker.stages.summarize import (
    chunk_transcript,
    format_transcript_lines,
    merge_synthesis_results,
    process_summarize_stage,
)

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


def test_format_transcript_lines():
    """Verify raw transcript snippets format into timestamped lines."""
    raw = [
        {"text": "Welcome to class", "start": 5.0, "duration": 2.0},
        {"text": "Here is the agenda", "start": 65.0, "duration": 3.0},
    ]
    formatted = format_transcript_lines(raw)
    assert "[00:05] Welcome to class" in formatted
    assert "[01:05] Here is the agenda" in formatted


def test_chunk_transcript():
    """Verify long transcripts split into logical chunks respecting line boundaries."""
    text = "line 1\nline 2\nline 3\nline 4\nline 5"
    chunks = chunk_transcript(text, max_chars=15)
    assert len(chunks) > 1
    assert all("\n" in c or "line" in c for c in chunks)


def test_merge_synthesis_results():
    """Verify merging chunks combines sections and deduplicates nearby keyframe timestamps."""
    res1 = SynthesisResult(
        overview="Part 1 overview.",
        sections=[SummarySection(section_title="Part 1", bullet_points=["Point 1"])],
        key_moments=[
            SlideKeyMoment(timestamp_sec=50, caption="Slide A caption"),
            SlideKeyMoment(timestamp_sec=100, caption="Slide B caption"),
        ],
    )
    res2 = SynthesisResult(
        overview="Part 2 overview.",
        sections=[SummarySection(section_title="Part 2", bullet_points=["Point 2"])],
        key_moments=[
            # Timestamp 52 is within 5 seconds of 50 -> must be deduplicated
            SlideKeyMoment(timestamp_sec=52, caption="Duplicate Slide A caption"),
            SlideKeyMoment(timestamp_sec=200, caption="Slide C caption"),
        ],
    )

    merged = merge_synthesis_results([res1, res2])
    assert "Part 1 overview. Part 2 overview." in merged.overview
    assert len(merged.sections) == 2
    # Out of 4 moments, 1 was duplicate (52 vs 50), leaving 3
    assert len(merged.key_moments) == 3
    timestamps = [m.timestamp_sec for m in merged.key_moments]
    assert timestamps == [50, 100, 200]


@pytest.mark.asyncio
async def test_process_summarize_stage_valid():
    """Verify summarize stage executes LLM synthesis, updates DB, and publishes to frame_extract."""
    async with get_test_db_session() as db_session:
        # Create initial job with raw transcript
        job = Job(
            video_id="sum_test_vid",
            youtube_url="https://www.youtube.com/watch?v=sum_test_vid",
            notion_token="secret_key",
            status=JobStatus.SUMMARIZING.value,
            progress=50,
            raw_transcript=[
                {"text": "Introduction to RabbitMQ", "start": 0.0, "duration": 5.0},
                {"text": "Look at this topology diagram", "start": 40.0, "duration": 10.0},
            ],
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="summarize",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
        )

        mock_adapter = MagicMock()
        mock_adapter.synthesize = AsyncMock(
            return_value=SynthesisResult(
                overview="Study notes on RabbitMQ architectures.",
                sections=[
                    SummarySection(
                        section_title="Brokers",
                        bullet_points=["Durable queues", "At least once delivery"],
                    )
                ],
                key_moments=[
                    SlideKeyMoment(
                        timestamp_sec=40,
                        caption="Architecture diagram showing exchange to queue bindings.",
                    )
                ],
            )
        )

        mock_limiter = MagicMock()
        mock_limiter.acquire = AsyncMock(return_value=True)

        mock_publish = AsyncMock()

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("worker.stages.summarize.publish_stage_task", mock_publish)
            success = await process_summarize_stage(
                task=task,
                session=db_session,
                rate_limiter=mock_limiter,
                llm_adapter=mock_adapter,
            )

        assert success is True

        # Check database updates
        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.EXTRACTING_FRAMES.value
        assert updated_job.progress == 75
        assert updated_job.llm_summary is not None
        assert "RabbitMQ architectures" in updated_job.llm_summary["overview"]
        assert len(updated_job.llm_summary["key_moments"]) == 1
        assert updated_job.llm_summary["key_moments"][0]["timestamp_sec"] == 40
        assert "Architecture diagram" in updated_job.llm_summary["key_moments"][0]["caption"]

        # Check next task dispatch
        mock_publish.assert_called_once()
        published_task: StageMessage = mock_publish.call_args[0][0]
        assert published_task.stage == "frame_extract"
        assert published_task.job_id == job.id


@pytest.mark.asyncio
async def test_process_summarize_stage_rate_limit_429_reraised():
    """Verify HTTP 429 / Rate Limit error is re-raised so worker backoff loop can handle it."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="rate_limit_vid",
            youtube_url="https://www.youtube.com/watch?v=rate_limit_vid",
            notion_token="secret_key",
            status=JobStatus.SUMMARIZING.value,
            raw_transcript=[{"text": "Sample text", "start": 0.0, "duration": 5.0}],
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="summarize",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
        )

        mock_adapter = MagicMock()
        mock_adapter.synthesize = AsyncMock(side_effect=Exception("HTTP 429: Resource has been exhausted"))

        mock_limiter = MagicMock()
        mock_limiter.acquire = AsyncMock(return_value=True)

        with pytest.raises(Exception) as exc_info:
            await process_summarize_stage(
                task=task,
                session=db_session,
                rate_limiter=mock_limiter,
                llm_adapter=mock_adapter,
            )

        assert "429" in str(exc_info.value)

