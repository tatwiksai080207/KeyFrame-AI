"""API route handlers for job submission."""

import logging
import re
from urllib.parse import parse_qs, urlparse

from fastapi import APIRouter, HTTPException, Response

from common.broker import StageMessage, publish_stage_task
from common.config import get_settings
from common.db import get_db_session
from common.models import Job, JobStatus
from gateway.schemas import JobCreateRequest, JobCreateResponse

logger = logging.getLogger("keyframe.gateway.routes")

router = APIRouter()

VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")
WATCH_HOSTS = {"www.youtube.com", "youtube.com", "m.youtube.com"}


def extract_video_id(url: str) -> str:
    """Returns the video ID from a youtube.com/watch or youtu.be URL, or raises ValueError."""
    parsed = urlparse(url.strip())
    video_id = None

    if parsed.hostname == "youtu.be":
        video_id = parsed.path.lstrip("/")
    elif parsed.hostname in WATCH_HOSTS and parsed.path == "/watch":
        video_id = parse_qs(parsed.query).get("v", [None])[0]

    if not video_id or not VIDEO_ID_PATTERN.match(video_id):
        raise ValueError("Could not find a valid YouTube video ID in the URL.")
    return video_id


@router.post("/jobs", response_model=JobCreateResponse, status_code=202)
async def create_job(payload: JobCreateRequest, response: Response):
    """Validates the URL, saves a QUEUED job, enqueues the first stage, returns 202."""
    try:
        video_id = extract_video_id(payload.youtube_url)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Use the caller's parent page or fall back to the default from settings.
    parent_id = payload.notion_parent_id or get_settings().notion_default_parent_page_id

    # Save the job. get_db_session() commits when the `with` block ends.
    async with get_db_session() as session:
        job = Job(
            video_id=video_id,
            youtube_url=payload.youtube_url,
            notion_token=payload.notion_token,
            notion_parent_id=parent_id,
        )
        session.add(job)
        job_id = job.id

    # Put the first task on the queue.
    message = StageMessage(
        job_id=job_id,
        stage="transcribe",
        video_id=video_id,
        youtube_url=payload.youtube_url,
        notion_token=payload.notion_token,
        notion_parent_id=parent_id,
    )
    try:
        await publish_stage_task(message)
    except Exception as exc:
        logger.error("Could not enqueue job %s: %r", job_id, exc)
        async with get_db_session() as session:
            stuck_job = await session.get(Job, job_id)
            stuck_job.transition_to(
                JobStatus.FAILED,
                error={"code": "ENQUEUE_FAILED", "message": "Could not reach the queue."},
            )
        raise HTTPException(status_code=503, detail="Could not queue the job. Try again shortly.")

    response.headers["Location"] = f"/jobs/{job_id}"
    logger.info("Job %s queued for video %s", job_id, video_id)
    return JobCreateResponse(job_id=job_id, status=JobStatus.QUEUED.value, video_id=video_id)