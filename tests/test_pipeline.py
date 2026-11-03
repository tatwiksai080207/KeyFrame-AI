"""End-to-end pipeline integration tests verifying seamless multi-stage execution."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from common.broker import StageMessage
from common.models import Base, Job, JobStatus
from worker.stages.transcribe import process_transcribe_stage
from worker.stages.summarize import process_summarize_stage
from worker.stages.frame_extract import process_frame_extract_stage
from worker.stages.publish import process_publish_stage
from worker.llm.base import SlideKeyMoment, SynthesisResult

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@asynccontextmanager
async def get_test_db_session():
    """Provides an isolated in-memory SQLite database session for pipeline tests."""
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
async def test_full_pipeline_end_to_end_success():
    """Executes the full pipeline sequentially through all four stages to completion."""
    async with get_test_db_session() as db_session:
        # Initial Job creation
        job = Job(
            video_id="e2e_video_101",
            youtube_url="https://www.youtube.com/watch?v=e2e_video_101",
            notion_token="secret_mock_notion_token",
            notion_parent_id="mock_parent_page_id",
            status=JobStatus.QUEUED.value,
            progress=0,
        )
        db_session.add(job)
        await db_session.commit()

        # ================= STAGE 1: TRANSCRIBE =================
        transcribe_task = StageMessage(
            job_id=job.id,
            stage="transcribe",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
            notion_parent_id=job.notion_parent_id,
        )

        mock_transcript = [
            {"text": "Welcome to the lecture on distributed computing.", "start": 0.0, "duration": 5.0},
            {"text": "Here we see the system architecture diagram.", "start": 60.0, "duration": 10.0},
            {"text": "Now look at the benchmark latency comparison.", "start": 180.0, "duration": 8.0},
        ]
        mock_publish_stage1 = AsyncMock()

        with patch(
            "worker.stages.transcribe.fetch_transcript_and_metadata",
            new=AsyncMock(return_value=(mock_transcript, 600, "Distributed Systems 101")),
        ), patch(
            "worker.stages.transcribe.publish_stage_task",
            new=mock_publish_stage1,
        ):
            success1 = await process_transcribe_stage(transcribe_task, db_session)

        assert success1 is True
        mock_publish_stage1.assert_called_once()
        summarize_task: StageMessage = mock_publish_stage1.call_args[0][0]
        assert summarize_task.stage == "summarize"

        # Verify DB after Stage 1
        res1 = await db_session.execute(select(Job).where(Job.id == job.id))
        job1 = res1.scalar_one()
        assert job1.status == JobStatus.SUMMARIZING.value
        assert job1.duration_sec == 600
        assert job1.video_title == "Distributed Systems 101"
        assert job1.progress == 50

        # ================= STAGE 2: SUMMARIZE =================
        mock_publish_stage2 = AsyncMock()
        mock_llm = MagicMock()
        mock_llm.synthesize = AsyncMock(
            return_value=SynthesisResult(
                overview="Distributed architecture and latency comparisons.",
                key_takeaways=["System architecture", "Low latency achieves high throughput"],
                detailed_sections=[
                    {
                        "heading": "Architecture Overview",
                        "content": "Explanation of broker and worker roles.",
                        "start_timestamp_sec": 0,
                    }
                ],
                key_moments=[
                    SlideKeyMoment(
                        timestamp_sec=60,
                        caption="Architecture diagram showing RabbitMQ message queues and workers.",
                    ),
                    SlideKeyMoment(
                        timestamp_sec=180,
                        caption="Benchmark bar graph comparing p99 latency across topologies.",
                    ),
                ],
            )
        )

        with patch("worker.stages.summarize.get_llm_adapter", return_value=mock_llm), patch(
            "worker.stages.summarize.publish_stage_task", mock_publish_stage2
        ):
            success2 = await process_summarize_stage(summarize_task, db_session)

        assert success2 is True
        mock_publish_stage2.assert_called_once()
        frame_task: StageMessage = mock_publish_stage2.call_args[0][0]
        assert frame_task.stage == "frame_extract"

        # Verify DB after Stage 2
        res2 = await db_session.execute(select(Job).where(Job.id == job.id))
        job2 = res2.scalar_one()
        assert job2.status == JobStatus.EXTRACTING_FRAMES.value
        assert job2.progress == 75
        assert len(job2.llm_summary["key_moments"]) == 2

        # ================= STAGE 3: FRAME EXTRACTION =================
        mock_publish_stage3 = AsyncMock()

        with patch("worker.stages.frame_extract.capture_frame_with_ffmpeg", return_value=True), patch(
            "worker.stages.frame_extract.publish_stage_task", mock_publish_stage3
        ):
            success3 = await process_frame_extract_stage(
                task=frame_task,
                session=db_session,
                stream_url_fn=lambda url: "https://mock.stream.manifest.m3u8",
            )

        assert success3 is True
        mock_publish_stage3.assert_called_once()
        publish_task: StageMessage = mock_publish_stage3.call_args[0][0]
        assert publish_task.stage == "publish"

        # Verify DB after Stage 3
        res3 = await db_session.execute(select(Job).where(Job.id == job.id))
        job3 = res3.scalar_one()
        assert job3.status == JobStatus.PUBLISHING.value
        assert job3.progress == 85
        assert len(job3.extracted_frames) == 2
        for frame in job3.extracted_frames:
            assert "caption" in frame
            assert "image_path" in frame

        # ================= STAGE 4: PUBLISH TO NOTION =================
        mock_notion = MagicMock()
        mock_notion.pages.create = AsyncMock(
            return_value={
                "id": "notion_page_uuid_999",
                "url": "https://notion.so/e2e-distributed-systems-101",
            }
        )

        success4 = await process_publish_stage(
            task=publish_task,
            session=db_session,
            notion_client_instance=mock_notion,
        )

        assert success4 is True
        mock_notion.pages.create.assert_called_once()

        # Verify Final Job State
        res4 = await db_session.execute(select(Job).where(Job.id == job.id))
        final_job = res4.scalar_one()
        assert final_job.status == JobStatus.COMPLETED.value
        assert final_job.progress == 100
        assert final_job.notion_page_url == "https://notion.so/e2e-distributed-systems-101"
        assert final_job.error is None


@pytest.mark.asyncio
async def test_full_pipeline_with_frame_degradation_completes_with_warnings():
    """Verifies that non-fatal frame extraction issues result in COMPLETED_WITH_WARNINGS."""
    async with get_test_db_session() as db_session:
        job = Job(
            video_id="e2e_warning_vid",
            youtube_url="https://www.youtube.com/watch?v=e2e_warning_vid",
            notion_token="secret_token",
            notion_parent_id="mock_parent_warning_id",
            status=JobStatus.QUEUED.value,
        )
        db_session.add(job)
        await db_session.commit()

        # Step 1: Transcribe succeeds
        transcribe_task = StageMessage(
            job_id=job.id,
            stage="transcribe",
            video_id=job.video_id,
            youtube_url=job.youtube_url,
            notion_token=job.notion_token,
            notion_parent_id=job.notion_parent_id,
        )
        mock_publish1 = AsyncMock()
        with patch(
            "worker.stages.transcribe.fetch_transcript_and_metadata",
            new=AsyncMock(return_value=([{"text": "Audio segment", "start": 0.0, "duration": 5.0}], 300, "Title")),
        ), patch("worker.stages.transcribe.publish_stage_task", mock_publish1):
            await process_transcribe_stage(transcribe_task, db_session)

        summarize_task = mock_publish1.call_args[0][0]

        # Step 2: Summarize succeeds with 2 key moments
        mock_publish2 = AsyncMock()
        mock_llm = MagicMock()
        mock_llm.synthesize = AsyncMock(
            return_value=SynthesisResult(
                overview="Overview text.",
                key_takeaways=["Point 1"],
                detailed_sections=[],
                key_moments=[
                    SlideKeyMoment(timestamp_sec=30, caption="First diagram."),
                    SlideKeyMoment(timestamp_sec=90, caption="Second diagram."),
                ],
            )
        )
        with patch("worker.stages.summarize.get_llm_adapter", return_value=mock_llm), patch(
            "worker.stages.summarize.publish_stage_task", mock_publish2
        ):
            await process_summarize_stage(summarize_task, db_session)

        frame_task = mock_publish2.call_args[0][0]

        # Step 3: Frame extraction partially succeeds (first frame captures, second fails)
        mock_publish3 = AsyncMock()

        def mock_capture(stream_url, timestamp, output_path):
            return timestamp == 30

        with patch("worker.stages.frame_extract.capture_frame_with_ffmpeg", side_effect=mock_capture), patch(
            "worker.stages.frame_extract.publish_stage_task", mock_publish3
        ):
            await process_frame_extract_stage(
                task=frame_task,
                session=db_session,
                stream_url_fn=lambda url: "https://mock.stream",
            )

        publish_task = mock_publish3.call_args[0][0]

        # Step 4: Notion publish succeeds
        mock_notion = MagicMock()
        mock_notion.pages.create = AsyncMock(
            return_value={"id": "page_warning_id", "url": "https://notion.so/page_with_warning"}
        )

        await process_publish_stage(
            task=publish_task,
            session=db_session,
            notion_client_instance=mock_notion,
        )

        # Verify DB state is COMPLETED_WITH_WARNINGS
        res = await db_session.execute(select(Job).where(Job.id == job.id))
        final_job = res.scalar_one()
        assert final_job.status == JobStatus.COMPLETED_WITH_WARNINGS.value
        assert final_job.progress == 100
        assert final_job.notion_page_url == "https://notion.so/page_with_warning"
        assert len(final_job.extracted_frames) == 1
        assert len(final_job.warnings) == 1
        assert final_job.warnings[0]["code"] == "FRAME_EXTRACTION_FAILED"
        assert final_job.error is None
