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
    callout_text = f"Source Video: {job.youtube_url} | Duration: {duration_min}m {duration_rem_sec}s"
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
    summary_data = job.llm_summary or {}
    overview = summary_data.get("overview")
    if overview:
        blocks.append(build_text_block("heading_2", "Overview"))
        blocks.append(build_text_block("paragraph", overview))

    # 3. Topic Sections with Bullet Points
    sections = summary_data.get("sections", [])
    for sec in sections:
        title = sec.get("section_title", "Key Insights")
        bullets = sec.get("bullet_points", [])
        blocks.append(build_text_block("heading_2", title))
        for bullet in bullets:
            blocks.append(build_text_block("bulleted_list_item", bullet))

    # 4. Visual Keyframes & Slides Section
    extracted_frames = job.extracted_frames or []
    if extracted_frames:
        blocks.append(build_text_block("heading_2", "Visual Slides and Diagrams"))
        for frame in extracted_frames:
            ts = frame.get("timestamp_sec", 0)
            caption = frame.get("caption", f"Frame captured at {ts}s")
            raw_path = frame.get("image_path", "")

            # Resolve public HTTPS URL for Notion block rendering
            if public_url_fn:
                image_url = public_url_fn(raw_path)
            elif settings.public_base_url:
                filename = os.path.basename(raw_path) or f"{job.id}_{ts}.jpg"
                image_url = f"{settings.public_base_url.rstrip('/')}/frames/{filename}"
            else:
                public_url = upload_frame_to_public_host(raw_path)
                image_url = public_url or f"http://localhost:8000/frames/{job.id}_{ts}.jpg"

            # Enforce Constraint B: frame image paired with explanatory caption
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

