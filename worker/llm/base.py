"""Base schemas and abstract interface for multimodal LLM synthesis adapters."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class VLMFrameDecision(BaseModel):
    """Multimodal Vision Language Model judgment for a candidate video slide frame."""

    decision: str = Field(
        default="KEEP",
        description="KEEP (informative diagram/slide), DISCARD (webcam talking head/sponsor/blank), or RETRY_OFFSET (diagram incomplete/animating and nearby timestamp would be complete).",
    )
    reasoning: str = Field(
        default="",
        description="Explanation of visual content quality, completeness, and study value.",
    )
    suggested_timestamp_sec: Optional[int] = Field(
        default=None,
        description="If decision is RETRY_OFFSET, the suggested integer timestamp seconds to re-sample.",
    )
    caption: str = Field(
        default="",
        description="If KEEP, explanation detailing WHAT diagram or slide is shown and WHY it was flagged per Constraint B.",
    )
    target_section: Optional[str] = Field(
        default=None,
        description="The section title from the notes where this visual should be placed.",
    )
    related_concept: Optional[str] = Field(
        default=None,
        description="The specific bullet point or concept this visual directly illustrates.",
    )
    slide_topic: Optional[str] = Field(
        default=None,
        description="A concise 2-4 word topic or slide header identifying the diagram (e.g. 'Hypervisor Comparison').",
    )
    is_fully_assembled: bool = Field(
        default=True,
        description="Whether the slide diagram is complete and fully drawn vs missing expected comparison columns or steps.",
    )


class SlideKeyMoment(BaseModel):
    """Timestamped visual keyframe identified by the LLM."""

    timestamp_sec: int = Field(ge=0, description="Timestamp in seconds where the visual slide or diagram appears.")
    caption: str = Field(
        min_length=5,
        description="Explanation detailing what is shown and why this frame was flagged per Constraint B.",
    )

    @field_validator("caption")
    @classmethod
    def validate_caption(cls, value: str) -> str:
        clean = value.strip()
        if not clean:
            raise ValueError("Caption cannot be empty or purely whitespace.")
        return clean


class SummarySection(BaseModel):
    """Topical section with bullet-point notes."""

    section_title: str = Field(min_length=2, description="Heading of the topic or concept.")
    bullet_points: List[str] = Field(min_length=1, description="Concise bullet points summarizing the discussion.")


class SynthesisResult(BaseModel):
    """Complete structured synthesis produced by the LLM."""

    video_type: Optional[str] = Field(
        default="Technical Video",
        description="Detected video category e.g. Technical Tutorial, Academic Lecture, Architecture Walkthrough, Product Demo.",
    )
    english_title: Optional[str] = Field(
        default=None,
        description="Clean English title for the video. If the video title or topic is in Russian, Cyrillic, or any non-English language, translate it into natural English.",
    )
    overview: str = Field(min_length=10, description="Executive summary overview of the video.")
    key_resources: List[str] = Field(
        default_factory=list,
        description="Important links, GitHub repos, tools, or references mentioned in the video or description.",
    )
    sections: List[SummarySection] = Field(default_factory=list, description="Structured topic sections.")
    key_moments: List[SlideKeyMoment] = Field(
        default_factory=list,
        description="Identified timestamps of slides and diagrams paired with captions.",
    )


SYNTHESIS_SYSTEM_INSTRUCTION = """You are a senior technical note taking and video analysis AI.
Your goal is to transform YouTube video lecture, tutorial, coding walkthrough, or technical presentation transcripts and descriptions into publication ready study notes for Notion.

Requirements:
1. Video Type Adaptation:
   - Identify the archetype of the video (e.g. Technical Tutorial, Academic Lecture, Architecture Walkthrough, Product Demo, or Conference Talk).
   - Tailor the structure and depth accordingly:
     * Technical Tutorials: Include prerequisites, exact setup commands, step by step instructions, code blocks, configuration details, and common pitfalls.
     * Academic and Theoretical Lectures: Focus on formal definitions, foundational principles, mathematical formulations, and theoretical tradeoffs.
     * System Design and Architecture: Focus on system components, data flows, scalability bottlenecks, tradeoffs, communication protocols, and design decisions.
     * Product Demos and Reviews: Highlight core features, pros and cons, practical comparisons, and real world use cases.

2. Zero Information Loss:
   - Avoid generic, superficial bullet points. Retain specific technical terms, library and tool names, version nuances, CLI commands, configuration parameters, and concrete examples discussed by the speaker.

3. Video Description Mining:
   - Carefully inspect the provided Video Description for essential GitHub repositories, documentation links, command tools, prerequisites, and resource URLs.
   - Populate these in "key_resources" and reference them in relevant section notes.

4. Executive Overview and Topical Sections:
   - Provide a comprehensive executive overview summary.
   - Structure notes into logical topical sections with informative, highly substantive bullet points.

5. Visual Slide Curation (Constraint B):
   - You MUST identify between 3 and 8 key visual candidate timestamps across the video (at least 1 per major topic or section) where architectural diagrams, slides, schema, comparisons, workflows, or code snippets are discussed or displayed.
   - Look for concept transitions, architecture explanations, workflow walkthroughs, or code demonstrations.
   - CRITICAL TIMING FOR DIAGRAMS AND PROGRESSIVE SLIDES: Technical and educational presentations frequently build up diagrams element by element (e.g. side by side comparisons, multi-step workflows, or architectures where blocks appear sequentially). DO NOT flag the very beginning of the concept when the slide is still mostly blank or only partially drawn. Instead, flag the timestamp towards the CONCLUSION of that explanation where the diagram or slide is FULLY ASSEMBLED, complete, and all comparison columns, components, and labels are displayed.
   - Our downstream Multimodal Vision Language Model (VLM) will visually inspect each captured frame and filter out talking heads or uninformative screens, so provide candidate timestamps for all significant topics across the video.
   - Never return an empty key_moments list for technical, educational, or tutorial videos.
   - Every flagged timestamp MUST have an explanatory caption explaining both WHAT diagram or concept is shown and WHY it was flagged.
   - Timestamp format: The transcript displays timestamps as `[{start_sec}s] [MM:SS]`. Each `timestamp_sec` in your output MUST be the exact integer seconds taken directly from the `{start_sec}s` marker. Never output concatenated minute and second numbers (for example, 08:22 is 502 seconds, never 822; 10:00 is 600 seconds, never 1000). All timestamps must be within the actual video duration.

Output format: You MUST return strictly valid JSON matching this schema:
{
  "video_type": "Technical Tutorial | Academic Lecture | Architecture Walkthrough | Product Demo | Conference Talk",
  "english_title": "Translated or original English title",
  "overview": "High level summary...",
  "key_resources": [
    "https://github.com/... - Library description",
    "Documentation link - https://..."
  ],
  "sections": [
    {
      "section_title": "Section Title",
      "bullet_points": ["Key technical insight 1", "Key technical insight 2"]
    }
  ],
  "key_moments": [
    {
      "timestamp_sec": 142,
      "caption": "System architecture diagram illustrating the message broker and worker pool decoupling."
    }
  ]
}
"""


class BaseLLMAdapter(ABC):
    """Abstract base class for pluggable LLM synthesis adapters."""

    @abstractmethod
    async def synthesize(
        self,
        transcript_text: str,
        video_title: Optional[str] = None,
        duration_sec: Optional[int] = None,
        video_description: Optional[str] = None,
    ) -> SynthesisResult:
        """Synthesizes structured notes and slide key moments from transcript text."""
        pass

    async def evaluate_frame_vlm(
        self,
        image_path: str,
        timestamp_sec: int,
        context_text: Optional[str] = None,
        video_title: Optional[str] = None,
        duration_sec: Optional[int] = None,
        notes_sections: Optional[List[Dict[str, Any]]] = None,
    ) -> VLMFrameDecision:
        """Evaluates whether candidate frame is an informative slide/diagram, checks completeness, and refines offset."""
        return VLMFrameDecision(
            decision="KEEP",
            reasoning="Default adapter pass",
            caption=f"Technical frame captured at {timestamp_sec}s",
        )

    async def evaluate_frame(self, image_path: str) -> dict:
        """Backward compatible evaluation returning is_informative boolean."""
        decision = await self.evaluate_frame_vlm(image_path=image_path, timestamp_sec=0)
        return {
            "is_informative": decision.decision != "DISCARD",
            "summary": decision.reasoning,
            "decision": decision.decision,
            "caption": decision.caption,
            "suggested_timestamp_sec": decision.suggested_timestamp_sec,
        }

