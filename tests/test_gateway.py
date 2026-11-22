"""Automated test suite verifying API Gateway endpoints, schemas, and static file serving."""

import os
import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from common.models import Base, Job, JobStatus
from gateway.main import app

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@asynccontextmanager
async def get_test_gateway_db():
    """Provides an isolated in-memory SQLite database session for gateway testing."""
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


@pytest.fixture
def test_client():
    """Creates an async HTTP test client targeting the FastAPI gateway application."""
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.mark.asyncio
async def test_post_job_valid_youtube_url_enqueued(test_client):
    """POST /jobs with valid YouTube URL creates database record, enqueues task, and returns 202."""
    mock_publish = AsyncMock()

    async with get_test_gateway_db() as db_session:
        @asynccontextmanager
        async def mock_db_ctx():
            yield db_session

        with patch("gateway.routes.get_db_session", mock_db_ctx), patch(
            "gateway.routes.publish_stage_task", mock_publish
        ):
            payload = {
                "youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                "notion_token": "secret_test_token_123",
                "notion_parent_id": "custom_parent_page_id",
            }
            response = await test_client.post("/jobs", json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "QUEUED"
    assert data["video_id"] == "dQw4w9WgXcQ"
    assert "job_id" in data
    assert response.headers.get("Location") == f"/jobs/{data['job_id']}"
    mock_publish.assert_called_once()
    published_message = mock_publish.call_args[0][0]
    assert published_message.job_id == data["job_id"]
    assert published_message.stage == "transcribe"


@pytest.mark.asyncio
async def test_post_job_short_url_and_trailing_slash(test_client):
    """POST /jobs with youtu.be short URL and trailing slash extracts correct 11-char video ID."""
    mock_publish = AsyncMock()

    async with get_test_gateway_db() as db_session:
        @asynccontextmanager
        async def mock_db_ctx():
            yield db_session

        with patch("gateway.routes.get_db_session", mock_db_ctx), patch(
            "gateway.routes.publish_stage_task", mock_publish
        ):
            payload = {
                "youtube_url": "https://youtu.be/dQw4w9WgXcQ/",
                "notion_token": "secret_test_token_123",
            }
            response = await test_client.post("/jobs", json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["video_id"] == "dQw4w9WgXcQ"


@pytest.mark.asyncio
async def test_post_job_invalid_url_rejected(test_client):
    """POST /jobs with non-YouTube or malformed URL returns HTTP 422."""
    async with get_test_gateway_db() as db_session:
        @asynccontextmanager
        async def mock_db_ctx():
            yield db_session

        with patch("gateway.routes.get_db_session", mock_db_ctx):
            payload = {
                "youtube_url": "https://vimeo.com/12345678",
                "notion_token": "secret_test_token_123",
            }
            response = await test_client.post("/jobs", json=payload)

    assert response.status_code == 422
    data = response.json()
    assert "error" in data
    assert data["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_get_job_status_and_not_found(test_client):
    """GET /jobs/{id} returns 200 for existing jobs and 404 for unknown job IDs."""
    async with get_test_gateway_db() as db_session:
        job = Job(
            video_id="dQw4w9WgXcQ",
            youtube_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            notion_token="secret_token",
            status=JobStatus.SUMMARIZING.value,
            progress=50,
            video_title="Test Video Title",
            duration_sec=300,
        )
        db_session.add(job)
        await db_session.commit()

        @asynccontextmanager
        async def mock_db_ctx():
            yield db_session

        with patch("gateway.routes.get_db_session", mock_db_ctx):
            # Test existing job
            resp = await test_client.get(f"/jobs/{job.id}")
            assert resp.status_code == 200
            data = resp.json()
            assert data["job_id"] == job.id
            assert data["status"] == "SUMMARIZING"
            assert data["progress"] == 50
            assert data["video_title"] == "Test Video Title"
            assert data["duration_sec"] == 300

            # Test unknown job returns 404
            unknown_resp = await test_client.get("/jobs/non-existent-id-000")
            assert unknown_resp.status_code == 404
            err_data = unknown_resp.json()
            assert err_data["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_get_job_results_lifecycle(test_client):
    """GET /jobs/{id}/results returns 409 while job is running, and 200 once completed."""
    async with get_test_gateway_db() as db_session:
        running_job = Job(
            video_id="running_vid",
            youtube_url="https://www.youtube.com/watch?v=running_vid",
            notion_token="secret_token",
            status=JobStatus.EXTRACTING_FRAMES.value,
            progress=75,
        )
        completed_job = Job(
            video_id="done_vid",
            youtube_url="https://www.youtube.com/watch?v=done_vid",
            notion_token="secret_token",
            status=JobStatus.COMPLETED.value,
            progress=100,
            notion_page_url="https://notion.so/test_completed_page",
            extracted_frames=[
                {"timestamp_sec": 30, "caption": "Diagram 1"},
                {"timestamp_sec": 90, "caption": "Diagram 2"},
            ],
            llm_summary={"key_moments": [{"timestamp_sec": 30}, {"timestamp_sec": 90}]},
        )
        db_session.add(running_job)
        db_session.add(completed_job)
        await db_session.commit()

        @asynccontextmanager
        async def mock_db_ctx():
            yield db_session

        with patch("gateway.routes.get_db_session", mock_db_ctx):
            # Running job returns 409 Conflict
            resp_running = await test_client.get(f"/jobs/{running_job.id}/results")
            assert resp_running.status_code == 409
            assert resp_running.json()["error"]["code"] == "JOB_NOT_FINISHED"

            # Completed job returns 200 OK with result
            resp_completed = await test_client.get(f"/jobs/{completed_job.id}/results")
            assert resp_completed.status_code == 200
            data = resp_completed.json()
            assert data["status"] == "COMPLETED"
            assert data["result"]["notion_page_url"] == "https://notion.so/test_completed_page"
            assert data["result"]["frames_requested"] == 2
            assert data["result"]["frames_published"] == 2


@pytest.mark.asyncio
async def test_static_frame_storage_serving(test_client, tmp_path):
    """Verifies that static frames mounted at /frames are served accurately."""
    from common.config import get_settings
    settings = get_settings()

    # Create a mock frame file in the frame storage directory
    os.makedirs(settings.frame_storage_dir, exist_ok=True)
    test_frame_name = "test_verify_frame_42.jpg"
    test_frame_path = os.path.join(settings.frame_storage_dir, test_frame_name)
    with open(test_frame_path, "wb") as f:
        f.write(b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x01\x00`\x00`\x00\x00\xFF\xDB")

    try:
        response = await test_client.get(f"/frames/{test_frame_name}")
        assert response.status_code == 200
        assert len(response.content) > 0
    finally:
        if os.path.exists(test_frame_path):
            os.remove(test_frame_path)


@pytest.mark.asyncio
async def test_notion_oauth_config_endpoint(test_client):
    """GET /auth/notion/config returns the configured client ID."""
    with patch("gateway.auth.get_settings") as mock_settings:
        mock_settings.return_value.notion_oauth_client_id = "test-client-id-abc-123"
        response = await test_client.get("/auth/notion/config")
        assert response.status_code == 200
        assert response.json()["client_id"] == "test-client-id-abc-123"

