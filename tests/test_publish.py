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


def test_construct_page_blocks_with_resources_and_inline_slides():
    """Verify construct_page_blocks renders video resources from description and embeds slides inline."""
    job = Job(
        video_id="resources_vid",
        youtube_url="https://www.youtube.com/watch?v=resources_vid",
        notion_token="secret_key",
        duration_sec=600,
        llm_summary={
            "video_type": "Technical Tutorial",
            "overview": "Comprehensive walkthrough of Docker microservices.",
            "key_resources": [
                "GitHub Repo: https://github.com/example/repo",
                "Official Docs: https://docs.example.com",
            ],
            "sections": [
                {
                    "section_title": "Container Setup",
                    "bullet_points": ["Run compose up", "Verify healthchecks"],
                }
            ],
            "key_moments": [
                {"timestamp_sec": 45, "caption": "Terminal showing docker compose output."}
            ],
        },
        extracted_frames=[
            {
                "timestamp_sec": 45,
                "image_path": "/frames/docker_45.jpg",
                "caption": "Terminal showing docker compose output.",
            }
        ],
    )

    blocks = construct_page_blocks(job)

    # Verify video type is rendered in header callout
    assert "Technical Tutorial" in blocks[0]["callout"]["rich_text"][0]["text"]["content"]

    # Verify Key Resources and References section exists
    headings = [b["heading_2"]["rich_text"][0]["text"]["content"] for b in blocks if b["type"] == "heading_2"]
    assert "Key Resources and References" in headings

    bullet_texts = [
        b["bulleted_list_item"]["rich_text"][0]["text"]["content"]
        for b in blocks
        if b["type"] == "bulleted_list_item"
    ]
    assert any("https://github.com/example/repo" in t for t in bullet_texts)

    # Verify image block is placed contextually
    image_blocks = [b for b in blocks if b["type"] == "image"]
    assert len(image_blocks) == 1
    assert image_blocks[0]["image"]["caption"][0]["text"]["content"] == "Terminal showing docker compose output."


def test_construct_page_blocks_interactive_links_and_tables():
    """Verify construct_page_blocks builds interactive Notion links, tables, dividers, and code blocks."""
    job = Job(
        video_id="interactive_vid",
        youtube_url="https://www.youtube.com/watch?v=interactive_vid",
        notion_token="secret_key",
        duration_sec=900,
        llm_summary={
            "video_type": "Architecture Walkthrough",
            "overview": "High level architecture of distributed microservices.",
            "key_resources": [
                "NGINX Ingress: https://kubernetes.github.io/ingress-nginx/",
                "Docker Compose Reference: https://docs.docker.com/compose/",
            ],
            "sections": [
                {
                    "section_title": "Gateway Configuration",
                    "bullet_points": [
                        "docker run -d -p 80:80 nginx:alpine",
                        "Configure upstream targets at https://api.internal/v1",
                    ],
                }
            ],
            "key_moments": [],
        },
        extracted_frames=[],
    )

    blocks = construct_page_blocks(job)

    # 1. Header callout must contain a clickable link to YouTube
    callout = blocks[0]
    assert callout["type"] == "callout"
    assert callout["callout"]["rich_text"][0]["text"]["link"]["url"] == "https://www.youtube.com/watch?v=interactive_vid"

    # 2. Check for Table blocks (Metadata table and Resources table)
    table_blocks = [b for b in blocks if b["type"] == "table"]
    assert len(table_blocks) >= 2

    # Verify first table is Metadata table
    meta_table = table_blocks[0]["table"]
    assert meta_table["table_width"] == 2
    assert meta_table["has_column_header"] is True
    # Rows should include Video Type, Runtime, Original Source, Topical Coverage
    assert len(meta_table["children"]) == 5  # 1 header + 4 rows

    # Verify second table is Resources table
    res_table = table_blocks[1]["table"]
    assert res_table["table_width"] == 3
    assert res_table["has_column_header"] is True

    # 3. Check for Divider blocks
    divider_blocks = [b for b in blocks if b["type"] == "divider"]
    assert len(divider_blocks) >= 1

    # 4. Check for Code block for CLI command
    code_blocks = [b for b in blocks if b["type"] == "code"]
    assert len(code_blocks) >= 1
    assert "docker run -d -p 80:80 nginx:alpine" in code_blocks[0]["code"]["rich_text"][0]["text"]["content"]
    assert code_blocks[0]["code"]["language"] == "bash"

    # 5. Check interactive link in bullet point
    bullets = [b for b in blocks if b["type"] == "bulleted_list_item"]
    api_bullet = next(b for b in bullets if "api.internal" in b["bulleted_list_item"]["rich_text"][0]["text"]["content"])
    assert api_bullet["bulleted_list_item"]["rich_text"][0]["text"]["link"]["url"] == "https://api.internal/v1"



