"""API route handlers for job submission."""

import logging
import re
from urllib.parse import parse_qs, urlparse

from fastapi import APIRouter, HTTPException, Response

from common.broker import StageMessage, publish_stage_task
from common.config import get_settings
from common.db import get_db_session
from common.models import Job, JobStatus
from gateway.schemas import JobCreateRequest, JobCreateResponse, JobResultResponse, JobStatusResponse

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

async def load_job(job_id: str) -> Job:
    """Fetches a job by ID or raises 404."""
    async with get_db_session() as session:
        job = await session.get(Job, job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")

    return job


def build_result(job: Job) -> dict:
    """Builds the final result payload."""
    requested = len((job.llm_summary or {}).get("key_moments", []))
    published = len(job.extracted_frames or [])

    return {
        "notion_page_url": job.notion_page_url,
        "frames_requested": requested,
        "frames_published": published,
    }


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Read-only and idempotent: polling never changes job state."""
    job = await load_job(job_id)

    result = None
    if JobStatus(job.status).is_terminal and job.notion_page_url:
        result = build_result(job)

    return JobStatusResponse(
        job_id=job.id,
        status=job.status,
        progress=job.progress,
        video_title=job.video_title,
        duration_sec=job.duration_sec,
        warnings=job.warnings or [],
        error=job.error,
        result=result,
    )


@router.get("/jobs/{job_id}/results", response_model=JobResultResponse)
async def get_job_results(job_id: str):
    """Returns 409 while running and 200 after a terminal state."""
    job = await load_job(job_id)

    if not JobStatus(job.status).is_terminal:
        raise HTTPException(
            status_code=409,
            detail=f"Job is still running (current stage: {job.status}).",
        )

    return JobResultResponse(
        status=job.status,
        result=build_result(job),
        warnings=job.warnings or [],
    )