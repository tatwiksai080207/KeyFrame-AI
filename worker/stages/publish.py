"""Stage 4: Notion study notes page assembly with structured blocks and slide captions."""

import logging
import os
from typing import Any, Callable, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.broker import StageMessage
from common.config import get_settings
from common.models import Job, JobStatus

logger = logging.getLogger(__name__)


def build_text_block(block_type: str, content: str) -> Dict[str, Any]:
    """Helper creating a standard Notion rich text block."""
    return {
        "object": "block",
        "type": block_type,
        block_type: {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": content},
                }
            ]
        },
    }


def build_image_block(image_url: str, caption_text: str) -> Dict[str, Any]:
    """Builds a Notion image block with attached LLM caption enforcing Constraint B."""
    return {
        "object": "block",
        "type": "image",
        "image": {
            "type": "external",
            "external": {"url": image_url},
            "caption": [
                {
                    "type": "text",
                    "text": {"content": caption_text},
                }
            ],
        },
    }


def upload_frame_to_public_host(image_path: str) -> Optional[str]:
    """Uploads a local image frame to a public image CDN so Notion cloud can embed it."""
    if not image_path or not os.path.exists(image_path):
        return None
    try:
        import httpx

        with open(image_path, "rb") as f:
            files = {"source": (os.path.basename(image_path), f.read(), "image/jpeg")}
            data = {"key": "6d207e02198a847aa98d0a2a901485a5", "action": "upload", "format": "json"}
            resp = httpx.post("https://freeimage.host/api/1/upload", data=data, files=files, timeout=15)
            if resp.status_code == 200:
                result = resp.json()
                url = (result.get("image") or {}).get("url")
                if url and url.startswith("http"):
                    return url
    except Exception as e:
        logger.warning(f"Could not upload frame {image_path} to public host: {e}")
    return None


def _resolve_image_url(
    raw_path: str,
    job_id: str,
    ts: int,
    settings: Any,
    public_url_fn: Optional[Callable[[str], str]] = None,
) -> str:
    """Helper resolving accessible HTTP/HTTPS URL for Notion block rendering."""
    if public_url_fn:
        return public_url_fn(raw_path)
    elif settings.public_base_url:
        filename = os.path.basename(raw_path) or f"{job_id}_{ts}.jpg"
        return f"{settings.public_base_url.rstrip('/')}/frames/{filename}"
    else:
        public_url = upload_frame_to_public_host(raw_path)
        return public_url or f"http://localhost:8000/frames/{job_id}_{ts}.jpg"


def construct_page_blocks(
    job: Job,
    public_url_fn: Optional[Callable[[str], str]] = None,
) -> List[Dict[str, Any]]:
    """Assembles structured Notion block tree: callouts, summary headings, and slide images."""
    settings = get_settings()
    blocks: List[Dict[str, Any]] = []

    # 1. Header Callout with metadata
    duration_min = (job.duration_sec or 0) // 60
    duration_rem_sec = (job.duration_sec or 0) % 60
    summary_data = job.llm_summary or {}
    video_type = summary_data.get("video_type", "Technical Video")
    callout_text = f"Source Video: {job.youtube_url} | Type: {video_type} | Duration: {duration_min}m {duration_rem_sec}s"
    blocks.append(
        {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": callout_text},
                    }
                ],
                "icon": {"type": "emoji", "emoji": "📝"},
            },
        }
    )

    # 2. Executive Overview
    overview = summary_data.get("overview")
    if overview:
        blocks.append(build_text_block("heading_2", "Overview"))
        blocks.append(build_text_block("paragraph", overview))

    # 3. Key Resources and References from Video Description
    key_resources = summary_data.get("key_resources", [])
    if key_resources:
        blocks.append(build_text_block("heading_2", "Key Resources and References"))
        for res_item in key_resources:
            blocks.append(build_text_block("bulleted_list_item", str(res_item)))

    # 4. Topic Sections with Contextual Inline Slides
    sections = summary_data.get("sections", [])
    extracted_frames = job.extracted_frames or []

    assigned_frame_paths = set()
    num_sections = len(sections)
    duration = job.duration_sec or 1200
    section_window = duration / max(num_sections, 1)

    for idx, sec in enumerate(sections):
        title = sec.get("section_title", "Key Insights")
        bullets = sec.get("bullet_points", [])
        blocks.append(build_text_block("heading_2", title))
        for bullet in bullets:
            blocks.append(build_text_block("bulleted_list_item", bullet))

        # Associate frames occurring within this topical window
        sec_start = idx * section_window
        sec_end = (idx + 1) * section_window
        title_keywords = [w for w in title.lower().split() if len(w) > 3]

        sec_frames = []
        for frame in extracted_frames:
            f_path = frame.get("image_path", "")
            if f_path in assigned_frame_paths:
                continue
            ts = frame.get("timestamp_sec", 0)
            caption_lower = frame.get("caption", "").lower()
            keyword_match = any(k in caption_lower for k in title_keywords)

            if (sec_start <= ts <= sec_end) or keyword_match:
                sec_frames.append(frame)
                assigned_frame_paths.add(f_path)

        for frame in sec_frames:
            ts = frame.get("timestamp_sec", 0)
            caption = frame.get("caption", f"Slide captured at {ts}s")
            raw_path = frame.get("image_path", "")
            image_url = _resolve_image_url(raw_path, job.id, ts, settings, public_url_fn)
            blocks.append(build_image_block(image_url=image_url, caption_text=caption))

    # 5. Remaining Visual Keyframes Gallery
    remaining_frames = [f for f in extracted_frames if f.get("image_path") not in assigned_frame_paths]
    if remaining_frames:
        blocks.append(build_text_block("heading_2", "Visual Slides and Diagrams"))
        for frame in remaining_frames:
            ts = frame.get("timestamp_sec", 0)
            caption = frame.get("caption", f"Frame captured at {ts}s")
            raw_path = frame.get("image_path", "")
            image_url = _resolve_image_url(raw_path, job.id, ts, settings, public_url_fn)
            blocks.append(build_image_block(image_url=image_url, caption_text=caption))

    return blocks


async def process_publish_stage(
    task: StageMessage,
    session: AsyncSession,
    notion_client_instance: Optional[Any] = None,
    public_url_fn: Optional[Callable[[str], str]] = None,
) -> bool:
    """Processes Stage 4: creates Notion page, embeds text and frames, and marks job terminal state."""
    settings = get_settings()

    # Load job from database
    result = await session.execute(select(Job).where(Job.id == task.job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise ValueError(f"Job with id {task.job_id} not found in database")

    # Determine Notion destination parent page ID
    target_parent_id = (
        task.notion_parent_id
        or job.notion_parent_id
        or settings.notion_default_parent_page_id
    )
    if not target_parent_id:
        job.transition_to(
            JobStatus.FAILED,
            error={
                "code": "MISSING_NOTION_PARENT_PAGE_ID",
                "message": (
                    "No Notion parent page ID provided in job submission or "
                    "NOTION_DEFAULT_PARENT_PAGE_ID environment variable."
                ),
            },
        )
        await session.commit()
        return False

    # Initialize Notion client
    if notion_client_instance is not None:
        notion = notion_client_instance
    else:
        from notion_client import AsyncClient

        notion = AsyncClient(auth=task.notion_token)

    # Build page children blocks
    children_blocks = construct_page_blocks(job, public_url_fn=public_url_fn)

    clean_parent_id = target_parent_id.replace("-", "")

    page_payload = {
        "parent": {"page_id": clean_parent_id},
        "properties": {
            "title": [
                {
                    "type": "text",
                    "text": {"content": job.video_title or f"Study Notes: {task.video_id}"},
                }
            ]
        },
        "children": children_blocks,
    }

    try:
        page_response = await notion.pages.create(**page_payload)
        page_url = page_response.get("url", f"https://notion.so/{clean_parent_id}")
    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate_limited" in error_str:
            logger.warning(f"Notion API rate limited (429) for job {task.job_id}: {e}")
            raise

        logger.error(f"Failed to publish Notion page for job {task.job_id}: {e}")
        job.transition_to(
            JobStatus.FAILED,
            error={"code": "NOTION_PUBLISH_FAILED", "message": f"Notion API call failed: {str(e)}"},
        )
        await session.commit()
        return False

    # Save created Notion URL
    job.notion_page_url = page_url

    # Check for warnings or omitted frames to determine terminal status
    summary_data = job.llm_summary or {}
    requested_frames = len(summary_data.get("key_moments", []))
    published_frames = len(job.extracted_frames or [])
    has_warnings = len(job.warnings or []) > 0 or (published_frames < requested_frames)

    target_status = (
        JobStatus.COMPLETED_WITH_WARNINGS if has_warnings else JobStatus.COMPLETED
    )

    # Advance to terminal status (progress 100%)
    job.transition_to(target_status, progress=100)
    await session.commit()
    return True

