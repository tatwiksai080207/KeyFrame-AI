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

    overview: str = Field(min_length=10, description="Executive summary overview of the video.")
    sections: List[SummarySection] = Field(default_factory=list, description="Structured topic sections.")
    key_moments: List[SlideKeyMoment] = Field(
        default_factory=list,
        description="Identified timestamps of slides and diagrams paired with captions.",
    )


SYNTHESIS_SYSTEM_INSTRUCTION = """You are a senior technical note-taking and video analysis AI.
Your goal is to transform YouTube video lecture or tutorial transcripts into structured, publication-ready study notes for Notion.

Requirements:
1. Provide an executive summary overview of the presentation.
2. Structure the core content into logical sections with clear, informative bullet points.
3. Identify all timestamps (in integer elapsed seconds) where visually important slides, architectural diagrams, whiteboards, or code snippets are discussed.
4. Constraint B Requirement: Every flagged timestamp MUST have an explanatory caption explaining both WHAT diagram/slide is shown and WHY it was flagged.
5. Timestamp format: The transcript displays timestamps as `[{start_sec}s] [MM:SS]`. Each `timestamp_sec` in your output MUST be the exact integer seconds taken directly from the `{start_sec}s` marker. Never output concatenated minute and second numbers (for example, 08:22 is 502 seconds, never 822; 10:00 is 600 seconds, never 1000). All timestamps must be within the actual video duration.

Output format: You MUST return strictly valid JSON matching this schema:
{
  "overview": "High-level summary...",
  "sections": [
    {
      "section_title": "Section Title",
      "bullet_points": ["Key insight 1", "Key insight 2"]
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
    ) -> SynthesisResult:
        """Synthesizes structured notes and slide key moments from transcript text."""
        pass

