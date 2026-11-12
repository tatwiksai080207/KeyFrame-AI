"""Stage 2: Multimodal LLM synthesis, transcript chunking, and slide caption generation."""

import logging
from typing import Any, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from aio_pika.abc import AbstractConnection

from common.broker import StageMessage, publish_stage_task
from common.config import get_settings
from common.models import Job, JobStatus
from common.rate_limiter import DistributedRateLimiter
from worker.llm import BaseLLMAdapter, SynthesisResult, get_llm_adapter
from worker.llm.base import SlideKeyMoment, SummarySection

logger = logging.getLogger(__name__)

# Max characters per transcript chunk before hierarchical merging is applied
MAX_CHUNK_CHARACTERS = 30000


def format_transcript_lines(snippets: List[Dict[str, Any]]) -> str:
    """Formats raw transcript items into human readable timestamped text lines."""
    formatted_lines = []
    for item in snippets:
        start_sec = int(item.get("start", 0))
        minutes = start_sec // 60
        seconds = start_sec % 60
        timestamp_str = f"[{start_sec}s] [{minutes:02d}:{seconds:02d}]"
        text = item.get("text", "").strip()
        if text:
            formatted_lines.append(f"{timestamp_str} {text}")
    return "\n".join(formatted_lines)


def chunk_transcript(formatted_transcript: str, max_chars: int = MAX_CHUNK_CHARACTERS) -> List[str]:
    """Splits long transcripts into sequential chunks respecting line boundaries."""
    if len(formatted_transcript) <= max_chars:
        return [formatted_transcript]

    lines = formatted_transcript.split("\n")
    chunks: List[str] = []
    current_chunk: List[str] = []
    current_length = 0

    for line in lines:
        line_len = len(line) + 1
        if current_length + line_len > max_chars and current_chunk:
            chunks.append("\n".join(current_chunk))
            current_chunk = []
            current_length = 0
        current_chunk.append(line)
        current_length += line_len

    if current_chunk:
        chunks.append("\n".join(current_chunk))

    return chunks


def merge_synthesis_results(results: List[SynthesisResult]) -> SynthesisResult:
    """Merges partial synthesis results chronologically, deduplicating nearby timestamps."""
    if not results:
        raise ValueError("Cannot merge empty synthesis results list.")
    if len(results) == 1:
        return results[0]

    all_overviews = [r.overview for r in results if r.overview]
    merged_overview = " ".join(all_overviews)

    merged_sections: List[SummarySection] = []
    for r in results:
        merged_sections.extend(r.sections)

    merged_moments: List[SlideKeyMoment] = []
    seen_timestamps = set()

    for r in results:
        for moment in r.key_moments:
            # Deduplicate key moments within 5 seconds of an already flagged moment
            is_duplicate = any(abs(moment.timestamp_sec - ts) <= 5 for ts in seen_timestamps)
            if not is_duplicate:
                seen_timestamps.add(moment.timestamp_sec)
                merged_moments.append(moment)

    # Sort key moments chronologically
    merged_moments.sort(key=lambda m: m.timestamp_sec)

    return SynthesisResult(
        overview=merged_overview,
        sections=merged_sections,
        key_moments=merged_moments,
    )


async def process_summarize_stage(
    task: StageMessage,
    session: AsyncSession,
    connection: Optional[AbstractConnection] = None,
    rate_limiter: Optional[DistributedRateLimiter] = None,
    llm_adapter: Optional[BaseLLMAdapter] = None,
) -> bool:
    """Processes Stage 2: coordinates LLM rate limit, synthesizes notes, and enqueues frame extraction."""
    settings = get_settings()

    # Load job from database
    result = await session.execute(select(Job).where(Job.id == task.job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise ValueError(f"Job with id {task.job_id} not found in database")

    raw_snippets = job.raw_transcript
    if not raw_snippets:
        job.transition_to(
            JobStatus.FAILED,
            error={"code": "NO_TRANSCRIPT_AVAILABLE", "message": "Job lacks transcript data for summarization."},
        )
        await session.commit()
        return False

    formatted_transcript = format_transcript_lines(raw_snippets)
    chunks = chunk_transcript(formatted_transcript)

    # Obtain rate limiter and LLM adapter
    limiter = rate_limiter or DistributedRateLimiter()
    adapter = llm_adapter or get_llm_adapter()

    partial_results: List[SynthesisResult] = []

    try:
        for chunk in chunks:
            # Respect global LLM rate limits across worker containers
            acquired = await limiter.acquire(
                key="llm_global_limit",
                max_rate=settings.llm_rate_limit_rpm,
                time_window_sec=60.0,
                block=True,
                timeout=45.0,
            )
            if not acquired:
                raise RuntimeError("Rate limit acquisition timed out waiting for available capacity.")

            chunk_res = await adapter.synthesize(
                transcript_text=chunk,
                video_title=job.video_title,
                duration_sec=job.duration_sec,
            )
            partial_results.append(chunk_res)

        final_synthesis = merge_synthesis_results(partial_results)

    except Exception as e:
        error_msg = str(e).lower()
        if "429" in error_msg or "resource_exhausted" in error_msg or "rate limit" in error_msg:
            # Bubble up HTTP 429 for worker consumer exponential backoff
            logger.warning(f"Rate limit encountered in summarize stage for job {task.job_id}: {e}")
            raise

        logger.error(f"LLM synthesis failed for job {task.job_id}: {e}")
        job.transition_to(
            JobStatus.FAILED,
            error={"code": "LLM_SYNTHESIS_ERROR", "message": f"LLM synthesis failed: {str(e)}"},
        )
        await session.commit()
        return False

    # Enforce Constraint B: verify all key moments have non-empty explanatory captions and valid timestamps
    verified_key_moments = []
    max_duration = job.duration_sec or 1200
    for moment in final_synthesis.key_moments:
        if not moment.caption or not moment.caption.strip():
            continue

        ts = moment.timestamp_sec

        # Auto-correct concatenated MMSS timestamps (e.g. 822 for 08:22 -> 502s)
        if ts > max_duration and ts >= 100:
            m = ts // 100
            s = ts % 100
            if s < 60:
                corrected = m * 60 + s
                if corrected <= max_duration:
                    logger.info(
                        f"Corrected concatenated timestamp {ts} to {corrected}s ({m:02d}:{s:02d})"
                    )
                    ts = corrected

        # Discard any keyframe timestamps exceeding the video duration
        if ts > max_duration or ts < 0:
            logger.warning(
                f"Discarding key moment at timestamp {ts} exceeding video duration {max_duration}s"
            )
            continue

        moment_dict = moment.model_dump()
        moment_dict["timestamp_sec"] = ts
        verified_key_moments.append(moment_dict)

    # Store synthesis outputs in database
    job.llm_summary = {
        "overview": final_synthesis.overview,
        "sections": [s.model_dump() for s in final_synthesis.sections],
        "key_moments": verified_key_moments,
    }

    # Transition to EXTRACTING_FRAMES stage
    job.transition_to(JobStatus.EXTRACTING_FRAMES)
    await session.commit()

    # Forward to Stage 3 (frame extract queue)
    next_task = StageMessage(
        job_id=task.job_id,
        stage="frame_extract",
        video_id=task.video_id,
        youtube_url=task.youtube_url,
        notion_token=task.notion_token,
        notion_parent_id=task.notion_parent_id,
        retry_count=0,
    )
    await publish_stage_task(next_task, connection=connection)
    return True

