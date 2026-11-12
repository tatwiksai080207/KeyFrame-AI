"""Stage 3: Direct stream keyframe extraction with ffmpeg and graceful degradation."""

import asyncio
import logging
import os
import shutil
import subprocess
from typing import Any, Callable, Dict, List, Optional, Tuple
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from aio_pika.abc import AbstractConnection

from common.broker import StageMessage, publish_stage_task
from common.config import get_settings
from common.models import Job, JobStatus

logger = logging.getLogger(__name__)


def extract_direct_stream_url(youtube_url: str) -> Optional[str]:
    """Retrieves direct HTTP media stream URL using yt-dlp without downloading."""
    try:
        import yt_dlp

        ydl_opts = {
            "format": "bestvideo[height<=720]/bestvideo/best[height<=720]/best",
            "quiet": True,
            "no_warnings": True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            if info:
                # Direct stream url is stored in url or requested_formats
                if "url" in info:
                    return info["url"]
                formats = info.get("formats", [])
                if formats:
                    return formats[-1].get("url")
    except Exception as e:
        logger.warning(f"Failed to extract stream URL via yt-dlp for {youtube_url}: {e}")
    return None


def capture_frame_with_ffmpeg(
    stream_url: str,
    timestamp_sec: int,
    output_path: str,
    timeout_sec: int = 30,
) -> bool:
    """Invokes ffmpeg to seek directly over HTTP and capture a single high quality frame."""
    ffmpeg_bin = shutil.which("ffmpeg") or "ffmpeg"
    cmd = [
        ffmpeg_bin,
        "-y",
        "-reconnect",
        "1",
        "-reconnect_at_eof",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "5",
        "-headers",
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n",
        "-ss",
        str(timestamp_sec),
        "-i",
        stream_url,
        "-frames:v",
        "1",
        "-q:v",
        "2",
        output_path,
    ]

    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_sec,
            check=False,
        )
        if res.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return True
        stderr_tail = res.stderr.decode("utf-8", errors="ignore")[-300:].strip()
        logger.warning(
            f"ffmpeg exited with code {res.returncode} for timestamp {timestamp_sec}: {stderr_tail}"
        )
    except Exception as e:
        logger.warning(f"ffmpeg capture timed out or failed for timestamp {timestamp_sec}: {e}")

    return False


async def process_frame_extract_stage(
    task: StageMessage,
    session: AsyncSession,
    connection: Optional[AbstractConnection] = None,
    stream_url_fn: Optional[Callable[[str], Optional[str]]] = None,
    capture_fn: Optional[Callable[[str, int, str], bool]] = None,
    storage_dir: Optional[str] = None,
) -> bool:
    """Processes Stage 3: captures frames for flagged timestamps with graceful degradation on failure."""
    settings = get_settings()
    target_storage = storage_dir or settings.frame_storage_dir
    os.makedirs(target_storage, exist_ok=True)

    # Load job from database
    result = await session.execute(select(Job).where(Job.id == task.job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise ValueError(f"Job with id {task.job_id} not found in database")

    llm_summary = job.llm_summary or {}
    key_moments: List[Dict[str, Any]] = llm_summary.get("key_moments", [])

    extracted_frames: List[Dict[str, Any]] = []

    if key_moments:
        resolve_stream = stream_url_fn or extract_direct_stream_url
        perform_capture = capture_fn or capture_frame_with_ffmpeg

        loop = asyncio.get_running_loop()
        stream_url = await loop.run_in_executor(None, resolve_stream, task.youtube_url)

        if not stream_url:
            # Stream URL could not be resolved; log warning for all frames and degrade gracefully
            for moment in key_moments:
                ts = moment.get("timestamp_sec", 0)
                job.add_warning(
                    code="FRAME_EXTRACTION_FAILED",
                    message="Unable to resolve video media stream for frame capture.",
                    timestamp_sec=ts,
                )
        else:
            for moment in key_moments:
                ts = moment.get("timestamp_sec", 0)
                caption = moment.get("caption", "")
                output_file = os.path.join(target_storage, f"{job.id}_{ts}.jpg")

                captured = await loop.run_in_executor(
                    None, perform_capture, stream_url, ts, output_file
                )

                if captured:
                    extracted_frames.append(
                        {
                            "timestamp_sec": ts,
                            "image_path": output_file,
                            "caption": caption,
                        }
                    )
                else:
                    # Graceful degradation per Section 6: record warning and continue
                    job.add_warning(
                        code="FRAME_EXTRACTION_FAILED",
                        message=f"Failed to capture frame at timestamp {ts}s.",
                        timestamp_sec=ts,
                    )

    # Store extracted frames in database
    job.extracted_frames = extracted_frames

    # Transition to PUBLISHING stage (progress 85%)
    job.transition_to(JobStatus.PUBLISHING, progress=85)
    await session.commit()

    # Forward to Stage 4 (publish queue)
    next_task = StageMessage(
        job_id=task.job_id,
        stage="publish",
        video_id=task.video_id,
        youtube_url=task.youtube_url,
        notion_token=task.notion_token,
        notion_parent_id=task.notion_parent_id,
        retry_count=0,
    )
    await publish_stage_task(next_task, connection=connection)
    return True

