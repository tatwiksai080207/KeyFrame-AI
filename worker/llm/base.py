"""Base schemas and abstract interface for multimodal LLM synthesis adapters."""

from abc import ABC, abstractmethod
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator


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
   - Identify timestamps (in integer elapsed seconds) where visually important slides, architectural diagrams, whiteboards, or code snippets are discussed.
   - ONLY flag timestamps that display actual technical diagrams, slide decks, or code on screen. Do NOT flag timestamps that show only the speaker talking head webcam or introductory splash screen without informational content.
   - Every flagged timestamp MUST have an explanatory caption explaining both WHAT diagram or slide is shown and WHY it was flagged.
   - Timestamp format: The transcript displays timestamps as `[{start_sec}s] [MM:SS]`. Each `timestamp_sec` in your output MUST be the exact integer seconds taken directly from the `{start_sec}s` marker. Never output concatenated minute and second numbers (for example, 08:22 is 502 seconds, never 822; 10:00 is 600 seconds, never 1000). All timestamps must be within the actual video duration.

Output format: You MUST return strictly valid JSON matching this schema:
{
  "video_type": "Technical Tutorial | Academic Lecture | Architecture Walkthrough | Product Demo | Conference Talk",
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

