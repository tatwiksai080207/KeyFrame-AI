"""Stage 1: Transcript extraction and Constraint A video duration enforcement."""

import asyncio
import math
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from aio_pika.abc import AbstractConnection
from youtube_transcript_api import (
    NoTranscriptFound,
    TranscriptsDisabled,
    YouTubeTranscriptApi,
)

from common.broker import StageMessage, publish_stage_task
from common.config import get_settings
from common.models import Job, JobStatus


def extract_metadata_from_ytdlp(youtube_url: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """Extracts video title, duration, and description using yt-dlp metadata inspection without downloading."""
    try:
        import yt_dlp

        ydl_opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            if info:
                duration = info.get("duration")
                title = info.get("title")
                description = info.get("description")
                return (int(duration) if duration is not None else None, title, description)
    except Exception:
        pass
    return None, None, None


async def fetch_transcript_and_metadata(
    video_id: str,
    youtube_url: str,
) -> Tuple[List[Dict[str, Any]], int, Optional[str], Optional[str]]:
    """Fetches timestamped captions and calculates true duration, title, and description."""
    # Run synchronous YouTubeTranscriptApi and yt-dlp in threadpool to avoid blocking async loop
    loop = asyncio.get_running_loop()

    def _sync_fetch():
        if hasattr(YouTubeTranscriptApi, "get_transcript"):
            return YouTubeTranscriptApi.get_transcript(video_id)

        api = YouTubeTranscriptApi()
        try:
            transcript_obj = api.list(video_id).find_transcript(["en", "en-US", "en-GB"])
            fetched = transcript_obj.fetch()
            return (
                fetched.to_raw_data()
                if hasattr(fetched, "to_raw_data")
                else [{"text": s.text, "start": s.start, "duration": s.duration} for s in fetched]
            )
        except Exception:
            # Fall back to any available transcript in the list
            available = api.list(video_id)
            for t in available:
                fetched = t.fetch()
                return (
                    fetched.to_raw_data()
                    if hasattr(fetched, "to_raw_data")
                    else [{"text": s.text, "start": s.start, "duration": s.duration} for s in fetched]
                )
            raise NoTranscriptFound(video_id, ["en"], None)

    transcript_list = await loop.run_in_executor(None, _sync_fetch)
    if not transcript_list:
        raise ValueError(f"No transcript items returned for video {video_id}")

    # Calculate duration from last snippet
    last_item = transcript_list[-1]
    calculated_duration = math.ceil(last_item["start"] + last_item.get("duration", 0.0))

    # Also attempt to obtain exact title, duration, and description from yt-dlp
    ytdlp_meta = await loop.run_in_executor(
        None, extract_metadata_from_ytdlp, youtube_url
    )
    ytdlp_duration = ytdlp_meta[0] if ytdlp_meta else None
    ytdlp_title = ytdlp_meta[1] if ytdlp_meta and len(ytdlp_meta) > 1 else None
    ytdlp_description = ytdlp_meta[2] if ytdlp_meta and len(ytdlp_meta) > 2 else None

    final_duration = ytdlp_duration if ytdlp_duration is not None else calculated_duration
    final_title = ytdlp_title or f"YouTube Video ({video_id})"
    final_description = ytdlp_description

    return transcript_list, final_duration, final_title, final_description


async def process_transcribe_stage(
    task: StageMessage,
    session: AsyncSession,
    connection: Optional[AbstractConnection] = None,
) -> bool:
    """Processes Stage 1: extracts transcript, enforces Constraint A, and enqueues Stage 2."""
    settings = get_settings()

    # Load job from database
    result = await session.execute(select(Job).where(Job.id == task.job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise ValueError(f"Job with id {task.job_id} not found in database")

    # Advance status to TRANSCRIBING
    if job.status == JobStatus.QUEUED.value:
        job.transition_to(JobStatus.TRANSCRIBING)
        await session.commit()

    try:
        fetch_res = await fetch_transcript_and_metadata(
            task.video_id, task.youtube_url
        )
        transcript = fetch_res[0]
        duration_sec = fetch_res[1]
        video_title = fetch_res[2]
        video_description = fetch_res[3] if len(fetch_res) > 3 else None
    except (TranscriptsDisabled, NoTranscriptFound) as e:
        job.transition_to(
            JobStatus.FAILED,
            error={
                "code": "TRANSCRIPT_NOT_FOUND",
                "message": f"Captions or subtitles are disabled or unavailable: {str(e)}",
            },
        )
        await session.commit()
        return False
    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "too many requests" in error_str:
            # External rate limit: bubble up so worker consumer can execute exponential backoff
            raise
        job.transition_to(
            JobStatus.FAILED,
            error={
                "code": "TRANSCRIPT_FETCH_ERROR",
                "message": f"Failed to retrieve video transcript: {str(e)}",
            },
        )
        await session.commit()
        return False

    # Enforce Hard Constraint A: Video length <= 20 minutes (1200 seconds)
    if duration_sec > settings.max_video_duration_sec:
        job.duration_sec = duration_sec
        job.video_title = video_title
        job.video_description = video_description
        job.transition_to(
            JobStatus.FAILED,
            error={
                "code": "VIDEO_TOO_LONG",
                "message": (
                    f"Video duration ({duration_sec}s) exceeds the maximum allowed length "
                    f"of {settings.max_video_duration_sec}s (20 minutes). Rejecting per Constraint A."
                ),
            },
        )
        await session.commit()
        return False

    # Store transcript, duration, title, and description
    job.raw_transcript = transcript
    job.duration_sec = duration_sec
    has_cyrillic = any(ord(c) > 127 for c in (video_title or ""))
    has_client_latin = bool(job.video_title and any(ord(c) <= 127 for c in job.video_title))
    if not (has_cyrillic and has_client_latin):
        job.video_title = video_title or job.video_title
    job.video_description = video_description

    # Transition to SUMMARIZING stage
    job.transition_to(JobStatus.SUMMARIZING)
    await session.commit()

    # Forward to Stage 2 (summarize queue)
    next_task = StageMessage(
        job_id=task.job_id,
        stage="summarize",
        video_id=task.video_id,
        youtube_url=task.youtube_url,
        notion_token=task.notion_token,
        notion_parent_id=task.notion_parent_id,
        retry_count=0,
    )
    await publish_stage_task(next_task, connection=connection)
    return True

