"""Tests for Stage 4: Notion page assembly, block generation, and Constraint B verification."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.stages.publish import construct_page_blocks, process_publish_stage

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


def test_construct_page_blocks_constraint_b():
    """Verify construct_page_blocks builds callout, summaries, and images with captions."""
    job = Job(
        video_id="notion_vid_1",
        youtube_url="https://www.youtube.com/watch?v=notion_vid_1",
        notion_token="secret_key",
        duration_sec=750,
        llm_summary={
            "overview": "Overview of message broker patterns.",
            "sections": [
                {
                    "section_title": "Durable Queuing",
                    "bullet_points": ["Message acks", "Crash recovery"],
                }
            ],
            "key_moments": [
                {"timestamp_sec": 120, "caption": "Topology architecture diagram."}
            ],
        },
        extracted_frames=[
            {
                "timestamp_sec": 120,
                "image_path": "/shared/frames/job_120.jpg",
                "caption": "Topology architecture diagram.",
            }
        ],
    )

    blocks = construct_page_blocks(job)
    assert len(blocks) >= 5

    # Check callout block
    assert blocks[0]["type"] == "callout"
    assert "notion_vid_1" in blocks[0]["callout"]["rich_text"][0]["text"]["content"]

    # Check Overview heading and paragraph
    assert blocks[1]["type"] == "heading_2"
    assert blocks[2]["type"] == "paragraph"
    assert "Overview of message broker patterns." in blocks[2]["paragraph"]["rich_text"][0]["text"]["content"]

    # Check image block with Constraint B caption
    image_blocks = [b for b in blocks if b["type"] == "image"]
    assert len(image_blocks) == 1
    assert "caption" in image_blocks[0]["image"]
    caption_content = image_blocks[0]["image"]["caption"][0]["text"]["content"]
    assert caption_content == "Topology architecture diagram."


@pytest.mark.asyncio
async def test_process_publish_stage_complete():
    """Verify job reaches COMPLETED when all frames are published with zero warnings."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="notion_test_success",
            youtube_url="https://www.youtube.com/watch?v=notion_test_success",
            notion_token="secret_key",
            status=JobStatus.PUBLISHING.value,
            progress=85,
            video_title="Distributed Systems Complete",
            duration_sec=600,
            llm_summary={
                "overview": "Study guide overview.",
                "key_moments": [{"timestamp_sec": 60, "caption": "Slide 1 caption"}],
            },
            extracted_frames=[
                {"timestamp_sec": 60, "image_path": "/frames/60.jpg", "caption": "Slide 1 caption"}
            ],
            warnings=[],
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="publish",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
            notion_parent_id="3fa85f6457174562b3fc2c963f66afa6",
        )

        mock_notion = MagicMock()
        mock_notion.pages = MagicMock()
        mock_notion.pages.create = AsyncMock(
            return_value={"url": "https://notion.so/test_published_page"}
        )

        success = await process_publish_stage(
            task=task,
            session=db_session,
            notion_client_instance=mock_notion,
        )

        assert success is True

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.COMPLETED.value
        assert updated_job.progress == 100
        assert updated_job.notion_page_url == "https://notion.so/test_published_page"


@pytest.mark.asyncio
async def test_process_publish_stage_with_warnings():
    """Verify job reaches COMPLETED_WITH_WARNINGS when warnings exist or frames were omitted."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="notion_test_warning",
            youtube_url="https://www.youtube.com/watch?v=notion_test_warning",
            notion_token="secret_key",
            status=JobStatus.PUBLISHING.value,
            progress=85,
            llm_summary={
                "overview": "Study guide overview.",
                "key_moments": [
                    {"timestamp_sec": 60, "caption": "Slide 1"},
                    {"timestamp_sec": 120, "caption": "Slide 2 (omitted)"},
                ],
            },
            # Only 1 of 2 frames was captured
            extracted_frames=[
                {"timestamp_sec": 60, "image_path": "/frames/60.jpg", "caption": "Slide 1"}
            ],
            warnings=[{"code": "FRAME_EXTRACTION_FAILED", "timestamp_sec": 120}],
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="publish",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
            notion_parent_id="3fa85f6457174562b3fc2c963f66afa6",
        )

        mock_notion = MagicMock()
        mock_notion.pages = MagicMock()
        mock_notion.pages.create = AsyncMock(
            return_value={"url": "https://notion.so/test_warning_page"}
        )

        success = await process_publish_stage(
            task=task,
            session=db_session,
            notion_client_instance=mock_notion,
        )

        assert success is True

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        # Terminal status must be COMPLETED_WITH_WARNINGS per Section 4 & 6
        assert updated_job.status == JobStatus.COMPLETED_WITH_WARNINGS.value
        assert updated_job.progress == 100
        assert updated_job.notion_page_url == "https://notion.so/test_warning_page"


@pytest.mark.asyncio
async def test_process_publish_stage_missing_parent():
    """Verify publish stage fails cleanly when no Notion parent page ID is provided."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="notion_test_no_parent",
            youtube_url="https://www.youtube.com/watch?v=notion_test_no_parent",
            notion_token="secret_key",
            status=JobStatus.PUBLISHING.value,
            progress=85,
        )
        db_session.add(job)
        await db_session.commit()

        task = StageMessage(
            job_id=job.id,
            stage="publish",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
            notion_parent_id=None,
        )

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("worker.stages.publish.get_settings", lambda: MagicMock(notion_default_parent_page_id=None))
            success = await process_publish_stage(task=task, session=db_session)

        assert success is False

        result = await db_session.execute(select(Job).where(Job.id == job.id))
        updated_job = result.scalar_one()

        assert updated_job.status == JobStatus.FAILED.value
        assert updated_job.error["code"] == "MISSING_NOTION_PARENT_PAGE_ID"

