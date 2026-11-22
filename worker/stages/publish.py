"""Stage 4: Notion study notes page assembly with structured blocks and slide captions."""

import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.broker import StageMessage
from common.config import get_settings
from common.models import Job, JobStatus

logger = logging.getLogger(__name__)


def parse_rich_text_with_links(content: str) -> List[Dict[str, Any]]:
    """Helper converting content into Notion rich text with clickable links."""
    content_str = str(content)
    md_match = re.match(r"^\[([^\]]+)\]\((https?://[^\s\)]+)\)$", content_str.strip())
    if md_match:
        label, url = md_match.groups()
        return [
            {
                "type": "text",
                "text": {"content": label, "link": {"url": url}},
                "annotations": {"bold": True},
            }
        ]

    url_match = re.search(r"https?://[^\s,\)\]]+", content_str)
    if url_match:
        url = url_match.group(0).rstrip(".,;:)")
        return [
            {
                "type": "text",
                "text": {"content": content_str, "link": {"url": url}},
            }
        ]

    return [
        {
            "type": "text",
            "text": {"content": content_str},
        }
    ]


def build_text_block(block_type: str, content: str) -> Dict[str, Any]:
    """Helper creating a standard Notion rich text block with clickable link support."""
    return {
        "object": "block",
        "type": block_type,
        block_type: {
            "rich_text": parse_rich_text_with_links(content),
        },
    }


def build_table_block(
    headers: List[str],
    rows: List[List[str]],
    has_column_header: bool = True,
) -> Dict[str, Any]:
    """Builds a Notion table block with children table_row blocks."""
    table_width = len(headers)
    children = []
    if has_column_header:
        children.append(
            {
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "text": {"content": h}, "annotations": {"bold": True}}]
                        for h in headers
                    ]
                },
            }
        )
    for row in rows:
        cells = []
        for val in row:
            cells.append(parse_rich_text_with_links(val))
        while len(cells) < table_width:
            cells.append([{"type": "text", "text": {"content": ""}}])
        children.append(
            {
                "type": "table_row",
                "table_row": {"cells": cells[:table_width]},
            }
        )
    return {
        "object": "block",
        "type": "table",
        "table": {
            "table_width": table_width,
            "has_column_header": has_column_header,
            "has_row_header": False,
            "children": children,
        },
    }


def build_code_block(code_content: str, language: str = "bash") -> Dict[str, Any]:
    """Builds a Notion code block with syntax highlighting."""
    return {
        "object": "block",
        "type": "code",
        "code": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": code_content},
                }
            ],
            "language": language,
        },
    }


def build_divider_block() -> Dict[str, Any]:
    """Builds a Notion horizontal divider block."""
    return {
        "object": "block",
        "type": "divider",
        "divider": {},
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


def assign_frames_to_sections_and_bullets(
    sections: List[Dict[str, Any]],
    extracted_frames: List[Dict[str, Any]],
    duration: int,
) -> Dict[int, Dict[int, List[Dict[str, Any]]]]:
    """Maps each extracted frame to an exact section and the specific bullet point it directly illustrates."""
    assignments: Dict[int, Dict[int, List[Dict[str, Any]]]] = {
        i: {} for i in range(len(sections))
    }
    if not sections or not extracted_frames:
        return assignments

    num_sections = len(sections)
    sec_window = duration / max(num_sections, 1)

    for frame in extracted_frames:
        f_path = frame.get("image_path", "")
        if not f_path:
            continue
        ts = frame.get("timestamp_sec", 0)
        target_sec = (frame.get("target_section") or "").lower().strip()
        related_concept = (frame.get("related_concept") or "").lower().strip()
        caption_lower = (frame.get("caption") or "").lower().strip()

        # Step 1: Find best matching section index
        best_sec_idx = None

        # Check explicit target_section match from VLM
        if target_sec:
            for s_idx, s in enumerate(sections):
                s_title = s.get("section_title", "").lower()
                if target_sec in s_title or s_title in target_sec:
                    best_sec_idx = s_idx
                    break

        # Check temporal window match
        if best_sec_idx is None:
            for s_idx in range(num_sections):
                start = s_idx * sec_window
                end = (s_idx + 1) * sec_window
                if start <= ts <= end:
                    best_sec_idx = s_idx
                    break

        # Check keyword match across section corpus
        if best_sec_idx is None:
            max_overlap = 0
            for s_idx, s in enumerate(sections):
                corpus = (
                    s.get("section_title", "")
                    + " "
                    + " ".join(s.get("bullet_points", []))
                ).lower()
                words = set(re.findall(r"[a-z0-9]{4,}", corpus))
                overlap = sum(
                    1 for w in words if w in caption_lower or (target_sec and w in target_sec)
                )
                if overlap > max_overlap:
                    max_overlap = overlap
                    best_sec_idx = s_idx

        if best_sec_idx is None:
            best_sec_idx = min(int(ts / max(sec_window, 1.0)), num_sections - 1)

        # Step 2: Find the exact bullet point within best_sec_idx to place the frame immediately AFTER
        bullets = sections[best_sec_idx].get("bullet_points", [])
        best_bullet_idx = 0
        if bullets:
            max_b_score = 0
            query = related_concept if related_concept else caption_lower
            query_words = set(re.findall(r"[a-z0-9]{4,}", query))

            for b_idx, b_text in enumerate(bullets):
                b_words = set(re.findall(r"[a-z0-9]{4,}", b_text.lower()))
                score = len(query_words.intersection(b_words))
                if score > max_b_score:
                    max_b_score = score
                    best_bullet_idx = b_idx

            # If no clear keyword winner, distribute by relative progress within the section window
            if max_b_score == 0:
                sec_start = best_sec_idx * sec_window
                rel_ts = max(0, ts - sec_start)
                ratio = min(1.0, rel_ts / max(sec_window, 1.0))
                best_bullet_idx = min(int(ratio * len(bullets)), len(bullets) - 1)

        if best_bullet_idx not in assignments[best_sec_idx]:
            assignments[best_sec_idx][best_bullet_idx] = []
        assignments[best_sec_idx][best_bullet_idx].append(frame)

    return assignments


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
                        "text": {
                            "content": callout_text,
                            "link": {"url": job.youtube_url} if job.youtube_url else None,
                        },
                    }
                ],
                "icon": {"type": "emoji", "emoji": "📝"},
            },
        }
    )

    # 2. Executive Overview
    overview = summary_data.get("overview")
    sections = summary_data.get("sections", [])
    if overview:
        blocks.append(build_text_block("heading_2", "Overview"))
        blocks.append(build_text_block("paragraph", overview))

        # Executive Metadata / Specifications Table
        sections_count = len(sections)
        metadata_rows = [
            ["Video Type", video_type],
            ["Runtime", f"{duration_min}m {duration_rem_sec}s"],
            ["Original Source", f"[{job.video_title or 'Watch on YouTube'}]({job.youtube_url})"],
            ["Topical Coverage", f"{sections_count} Architectural Sections"],
        ]
        blocks.append(
            build_table_block(
                headers=["Attribute", "Details"],
                rows=metadata_rows,
            )
        )

    # 3. Key Resources and References from Video Description
    key_resources = summary_data.get("key_resources", [])
    if key_resources:
        blocks.append(build_divider_block())
        blocks.append(build_text_block("heading_2", "Key Resources and References"))
        resource_table_rows = []
        for res_item in key_resources:
            res_str = str(res_item)
            blocks.append(build_text_block("bulleted_list_item", res_str))

            # Derive table row
            url_match = re.search(r"https?://[^\s,\)\]]+", res_str)
            if url_match:
                url = url_match.group(0).rstrip(".,;:)")
                desc = res_str.replace(url, "").strip(" -:–|") or "Reference Link"
                cat = (
                    "Video Tutorial"
                    if "youtu" in url
                    else (
                        "GitHub Repo"
                        if "github" in url
                        else ("Documentation" if "doc" in url else "Resource")
                    )
                )
                resource_table_rows.append([desc, cat, f"[Open Resource ↗]({url})"])
            else:
                resource_table_rows.append([res_str, "General Reference", "N/A"])

        if resource_table_rows:
            blocks.append(
                build_table_block(
                    headers=["Resource", "Category", "Link"],
                    rows=resource_table_rows,
                )
            )

    # 4. Topic Sections with Contextual In-Text Slides
    extracted_frames = job.extracted_frames or []
    duration = job.duration_sec or 1200
    frame_assignments = assign_frames_to_sections_and_bullets(sections, extracted_frames, duration)

    for idx, sec in enumerate(sections):
        blocks.append(build_divider_block())
        title = sec.get("section_title", "Key Insights")
        bullets = sec.get("bullet_points", [])
        blocks.append(build_text_block("heading_2", title))

        sec_map = frame_assignments.get(idx, {})

        if not bullets and 0 in sec_map:
            for f in sec_map[0]:
                ts = f.get("timestamp_sec", 0)
                caption = f.get("caption", f"Slide captured at {ts}s")
                raw_path = f.get("image_path", "")
                image_url = _resolve_image_url(raw_path, job.id, ts, settings, public_url_fn)
                blocks.append(build_image_block(image_url=image_url, caption_text=caption))

        for bullet_idx, bullet in enumerate(bullets):
            b_str = str(bullet).strip()
            if b_str.startswith("```") and b_str.endswith("```"):
                code_text = b_str.strip("`").strip()
                blocks.append(build_code_block(code_text, "bash"))
            elif any(
                b_str.startswith(cmd)
                for cmd in ["docker ", "kubectl ", "npm ", "git ", "pip ", "curl ", "sudo "]
            ):
                blocks.append(build_code_block(b_str, "bash"))
            else:
                blocks.append(build_text_block("bulleted_list_item", b_str))

            # Embed assigned image(s) IMMEDIATELY UNDER this bullet point in the text
            if bullet_idx in sec_map:
                for f in sec_map[bullet_idx]:
                    ts = f.get("timestamp_sec", 0)
                    caption = f.get("caption", f"Slide captured at {ts}s")
                    raw_path = f.get("image_path", "")
                    image_url = _resolve_image_url(raw_path, job.id, ts, settings, public_url_fn)
                    blocks.append(build_image_block(image_url=image_url, caption_text=caption))

    # Fallback for any frames not placed within sections (or if no sections were generated)
    placed_paths = {
        f.get("image_path")
        for sec_map in frame_assignments.values()
        for b_frames in sec_map.values()
        for f in b_frames
        if f.get("image_path")
    }
    unplaced_frames = [
        f for f in extracted_frames if f.get("image_path") and f.get("image_path") not in placed_paths
    ]
    if unplaced_frames:
        blocks.append(build_divider_block())
        blocks.append(build_text_block("heading_2", "Additional Visual References"))
        for f in unplaced_frames:
            ts = f.get("timestamp_sec", 0)
            caption = f.get("caption", f"Slide captured at {ts}s")
            raw_path = f.get("image_path", "")
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

    # Idempotency safeguard: if a previous attempt already created the Notion page before a worker crash, reuse it
    if job.notion_page_url:
        logger.info(
            f"Job {job.id} already has Notion page {job.notion_page_url}; reusing for idempotency."
        )
        page_url = job.notion_page_url
    else:
        try:
            page_response = await notion.pages.create(**page_payload)
            page_url = page_response.get("url", f"https://notion.so/{clean_parent_id}")
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "rate_limited" in error_str:
                logger.warning(f"Notion API rate limited (429) for job {task.job_id}: {e}")
                raise

            page_created = False
            if "could not find page" in error_str or "validation_error" in error_str:
                try:
                    db_payload = {
                        "parent": {"database_id": clean_parent_id},
                        "properties": {
                            "Name": {
                                "title": [
                                    {
                                        "type": "text",
                                        "text": {"content": job.video_title or f"Study Notes: {task.video_id}"},
                                    }
                                ]
                            }
                        },
                        "children": children_blocks,
                    }
                    page_response = await notion.pages.create(**db_payload)
                    page_url = page_response.get("url", f"https://notion.so/{clean_parent_id}")
                    page_created = True
                except Exception as db_err:
                    logger.warning(f"Database parent attempt failed: {db_err}")

            if not page_created:
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

