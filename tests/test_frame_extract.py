"""Tests for Stage 3: direct stream frame extraction, graceful degradation, and warning tracking."""

import os
import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch
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


def test_dhash_and_blank_detection_edge_cases(tmp_path):
    """Verify difference hash and blank frame detector handle missing or empty files cleanly."""
    from worker.stages.frame_extract import compute_image_dhash, is_blank_or_solid_frame

    non_existent = str(tmp_path / "missing.jpg")
    assert compute_image_dhash(non_existent) is None
    assert is_blank_or_solid_frame(non_existent) is True

    empty_file = tmp_path / "empty.jpg"
    empty_file.write_bytes(b"")
    assert compute_image_dhash(str(empty_file)) is None
    assert is_blank_or_solid_frame(str(empty_file)) is True


@pytest.mark.asyncio
async def test_verify_and_curate_frame_offset_and_vision_fallback(tmp_path):
    """Verify verify_and_curate_frame evaluates vision feedback and deduplicates slides."""
    from unittest.mock import MagicMock
    from worker.stages.frame_extract import verify_and_curate_frame

    mock_adapter = MagicMock()
    # First candidate at ts=10 is talking head; second candidate at ts=15 is informative slide
    mock_adapter.evaluate_frame = AsyncMock(
        side_effect=[
            {"is_informative": False, "summary": "talking head"},
            {"is_informative": True, "summary": "technical architecture diagram"},
        ]
    )

    def mock_capture(url, ts, out_path):
        # Write dummy byte data so file exists on disk
        with open(out_path, "wb") as f:
            f.write(b"dummy_image_data")
        return True

    with patch("worker.stages.frame_extract.is_blank_or_solid_frame", return_value=False), \
         patch("worker.stages.frame_extract.compute_image_dhash", side_effect=[12345, 67890]):

        result = await verify_and_curate_frame(
            stream_url="https://test.googlevideo.com/playback",
            target_storage=str(tmp_path),
            job_id="curate_job_1",
            timestamp_sec=10,
            caption="Architecture slide",
            video_duration=600,
            seen_hashes=[],
            capture_fn=mock_capture,
            llm_adapter=mock_adapter,
        )

        assert result is not None
        chosen_path, chosen_ts, chosen_hash = result
        # Must have advanced to offset +5 (ts=15) because ts=10 was talking head
        assert chosen_ts == 15
        assert chosen_hash == 67890


def test_get_transcript_context_around():
    """Verify context extraction extracts text within temporal window."""
    from worker.stages.frame_extract import get_transcript_context_around

    assert get_transcript_context_around(None, 50) == ""
    assert get_transcript_context_around([], 50) == ""

    snippets = [
        {"start": 10.0, "text": "Welcome to the lecture"},
        {"start": 45.0, "text": "Notice how the broker routes messages"},
        {"start": 55.0, "text": "Here is the architectural diagram"},
        {"start": 120.0, "text": "In conclusion for today"},
    ]
    # At ts=50 with default window=20s, snippets at 45.0 and 55.0 are captured
    ctx = get_transcript_context_around(snippets, 50, window_sec=20.0)
    assert "Notice how the broker routes messages" in ctx
    assert "Here is the architectural diagram" in ctx
    assert "Welcome to the lecture" not in ctx
    assert "In conclusion" not in ctx


@pytest.mark.asyncio
async def test_verify_and_curate_frame_vlm_keep_and_retry(tmp_path):
    """Verify verify_and_curate_frame handles VLM KEEP and RETRY_OFFSET decisions."""
    from unittest.mock import AsyncMock, MagicMock
    from worker.llm.base import VLMFrameDecision
    from worker.stages.frame_extract import verify_and_curate_frame

    def mock_capture(url, ts, out_path):
        with open(out_path, "wb") as f:
            f.write(b"frame_bytes")
        return True

    # 1. Test KEEP
    mock_adapter_keep = MagicMock()
    mock_adapter_keep.evaluate_frame_vlm = AsyncMock(
        return_value=VLMFrameDecision(
            decision="KEEP",
            reasoning="Crisp database schema diagram",
            caption="WHAT: High-level database schema. WHY: Crucial for understanding foreign key relations.",
        )
    )

    with patch("worker.stages.frame_extract.is_blank_or_solid_frame", return_value=False), \
         patch("worker.stages.frame_extract.compute_image_dhash", return_value=11111):

        res_keep = await verify_and_curate_frame(
            stream_url="https://test.googlevideo.com/playback",
            target_storage=str(tmp_path),
            job_id="job_vlm_keep",
            timestamp_sec=30,
            caption="Initial caption",
            video_duration=300,
            seen_hashes=[],
            capture_fn=mock_capture,
            llm_adapter=mock_adapter_keep,
        )

        assert res_keep is not None
        path, ts, dhash = res_keep
        assert ts == 30
        assert dhash == 11111
        assert "database schema" in res_keep.caption

    # 2. Test RETRY_OFFSET
    mock_adapter_retry = MagicMock()
    mock_adapter_retry.evaluate_frame_vlm = AsyncMock(
        return_value=VLMFrameDecision(
            decision="RETRY_OFFSET",
            reasoning="Animation in progress, final state visible at 35s",
            suggested_timestamp_sec=35,
            caption="WHAT: Complete pipeline animation. WHY: Shows full queue flow.",
        )
    )

    with patch("worker.stages.frame_extract.is_blank_or_solid_frame", return_value=False), \
         patch("worker.stages.frame_extract.compute_image_dhash", return_value=22222):

        res_retry = await verify_and_curate_frame(
            stream_url="https://test.googlevideo.com/playback",
            target_storage=str(tmp_path),
            job_id="job_vlm_retry",
            timestamp_sec=30,
            caption="Initial caption",
            video_duration=300,
            seen_hashes=[],
            capture_fn=mock_capture,
            llm_adapter=mock_adapter_retry,
        )

        assert res_retry is not None
        path, ts, dhash = res_retry
        assert ts == 35
        assert dhash == 22222
        assert "Complete pipeline animation" in res_retry.caption


@pytest.mark.asyncio
async def test_process_frame_extract_fallback_when_key_moments_empty(tmp_path):
    """Verify frame extract synthesizes candidate timestamps across sections when key_moments is empty."""
    from common.broker import StageMessage
    from common.models import Job, JobStatus
    from worker.stages.frame_extract import process_frame_extract_stage
    from tests.test_publish import get_test_db_session

    async with get_test_db_session() as db_session:
        job = Job(
            video_id="fallback_vid_1",
            youtube_url="https://www.youtube.com/watch?v=fallback_vid_1",
            notion_token="dummy_notion_token",
            status=JobStatus.EXTRACTING_FRAMES.value,
            progress=50,
            duration_sec=600,
            llm_summary={
                "overview": "Overview of proxies.",
                "sections": [
                    {"section_title": "Forward Proxy", "bullet_points": ["Filtering"]},
                    {"section_title": "Reverse Proxy", "bullet_points": ["Load balancing"]},
                ],
                "key_moments": [],  # Empty key moments from LLM!
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

        def mock_capture(url, ts, out_path):
            with open(out_path, "wb") as f:
                f.write(b"frame_data")
            return True

        from unittest.mock import AsyncMock, MagicMock
        mock_adapter = MagicMock()
        from worker.llm.base import VLMFrameDecision
        mock_adapter.evaluate_frame_vlm = AsyncMock(
            return_value=VLMFrameDecision(
                decision="KEEP",
                reasoning="Clear architecture slide",
                caption="WHAT: System diagram. WHY: Visualizes key architectural flows.",
            )
        )

        with patch("worker.stages.frame_extract.is_blank_or_solid_frame", return_value=False), \
             patch("worker.stages.frame_extract.compute_image_dhash", side_effect=[10101, 20202, 30303, 40404]):

            success = await process_frame_extract_stage(
                task=task,
                session=db_session,
                stream_url_fn=lambda url: "https://playback.test/stream",
                capture_fn=mock_capture,
                storage_dir=str(tmp_path),
                llm_adapter=mock_adapter,
            )

            assert success is True

            from sqlalchemy import select
            result = await db_session.execute(select(Job).where(Job.id == job.id))
            updated_job = result.scalar_one()

            # Must have synthesized candidate frames from the 2 sections despite key_moments being empty
            assert len(updated_job.extracted_frames) == 2
            assert updated_job.status == JobStatus.PUBLISHING.value


def test_deduplicate_progressive_slides():
    """Verifies that earlier partial builds of the same slide/topic are dropped in favor of fully assembled diagrams."""
    from worker.stages.frame_extract import deduplicate_progressive_slides

    candidate_frames = [
        {
            "timestamp_sec": 30,
            "image_path": "/storage/frame_30.jpg",
            "caption": "WHAT: Concept introduction diagram showing a physical computer. WHY: Explains baseline before virtualization.",
            "slide_topic": "Virtual Machine Architecture",
            "target_section": "Introduction to Virtual Machines",
            "related_concept": "Baseline physical computer hardware and OS",
        },
        {
            "timestamp_sec": 75,
            "image_path": "/storage/frame_75.jpg",
            "caption": "WHAT: Full conceptual diagram illustrating virtualization architecture with guest operating systems. WHY: Demystifies hardware abstraction.",
            "slide_topic": "Virtual Machine Architecture",
            "target_section": "Introduction to Virtual Machines",
            "related_concept": "Virtualization layer running multiple guest OS",
        },
        {
            "timestamp_sec": 320,
            "image_path": "/storage/frame_320.jpg",
            "caption": "WHAT: Hypervisor classification comparing Type 1 and Type 2. WHY: Delineates bare metal vs hosted systems.",
            "slide_topic": "Hypervisor Architecture",
            "target_section": "Hypervisors",
            "related_concept": "Type 1 vs Type 2 hypervisors",
        },
    ]

    deduped = deduplicate_progressive_slides(candidate_frames)
    # Earlier frame at 30s should be dropped; only 75s and 320s remain
    assert len(deduped) == 2
    assert deduped[0]["timestamp_sec"] == 75
    assert deduped[1]["timestamp_sec"] == 320





