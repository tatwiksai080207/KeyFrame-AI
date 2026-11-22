"""OpenAI synthesis adapter implementation."""

import base64
import json
import os
from typing import Any, Dict, List, Optional
from common.config import get_settings
from worker.llm.base import (
    SYNTHESIS_SYSTEM_INSTRUCTION,
    BaseLLMAdapter,
    SynthesisResult,
    VLMFrameDecision,
)
from worker.llm.gemini import clean_json_text


class OpenAIAdapter(BaseLLMAdapter):
    """OpenAI GPT synthesis adapter utilizing JSON object response format."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> None:
        settings = get_settings()
        self.api_key = api_key or settings.openai_api_key
        if not self.api_key:
            raise ValueError(
                "OpenAI API key is required. Set OPENAI_API_KEY in your environment or .env file."
            )
        self.model_name = model_name or (
            settings.llm_model if "gpt" in settings.llm_model else "gpt-4o-mini"
        )

        from openai import AsyncOpenAI

        self.client = AsyncOpenAI(api_key=self.api_key)

    async def synthesize(
        self,
        transcript_text: str,
        video_title: Optional[str] = None,
        duration_sec: Optional[int] = None,
        video_description: Optional[str] = None,
    ) -> SynthesisResult:
        """Invokes OpenAI model asynchronously to synthesize study notes and keyframes."""
        duration_info = f"\nVideo Duration: {duration_sec} seconds" if duration_sec else ""
        desc_info = f"\nVideo Description:\n{video_description}\n" if video_description else ""
        prompt = (
            f"Video Title: {video_title or 'Untitled Video'}{duration_info}\n"
            f"{desc_info}\n"
            f"Timestamped Transcript Content:\n{transcript_text}\n\n"
            f"Reminder: Each timestamp_sec must be an exact integer seconds value taken directly from the [{'{seconds}'}s] markers and must not exceed the video duration. Never output concatenated minute and second digits."
        )

        response = await self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": SYNTHESIS_SYSTEM_INSTRUCTION},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )

        choice = response.choices[0]
        content = choice.message.content
        if not content:
            raise ValueError("Empty response received from OpenAI API.")

        raw_json = clean_json_text(content)
        data = json.loads(raw_json)
        return SynthesisResult(**data)

    async def evaluate_frame_vlm(
        self,
        image_path: str,
        timestamp_sec: int,
        context_text: Optional[str] = None,
        video_title: Optional[str] = None,
        duration_sec: Optional[int] = None,
        notes_sections: Optional[List[Dict[str, Any]]] = None,
    ) -> VLMFrameDecision:
        """Uses OpenAI Vision to evaluate slide relevance, completeness, captions, and in-text placement."""
        if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
            return VLMFrameDecision(decision="DISCARD", reasoning="Empty or missing image.")

        try:
            with open(image_path, "rb") as f:
                b64_img = base64.b64encode(f.read()).decode("utf-8")

            title_context = f'Video Title: "{video_title}"\n' if video_title else ""
            transcript_context = f'Surrounding Transcript:\n"{context_text}"\n' if context_text else ""
            sections_context = ""
            if notes_sections and isinstance(notes_sections, list):
                sec_lines = [
                    f"- {s.get('section_title', '')}: " + "; ".join(s.get('bullet_points', [])[:3])
                    for s in notes_sections
                    if isinstance(s, dict)
                ]
                if sec_lines:
                    sections_context = "Current Notes Sections:\n" + "\n".join(sec_lines) + "\n\n"

            prompt = (
                f"{title_context}{transcript_context}{sections_context}"
                f"Candidate frame extracted at: {timestamp_sec} seconds.\n\n"
                "Evaluate this video frame for study notes:\n"
                "1. STRICT VALUE CHECK: Does it show an authentic, non-trivial technical slide, architecture diagram, or code vs a talking head face, sponsor card, or generic background? If it does not add necessary visual insight, select 'DISCARD'.\n"
                "2. COMPLETENESS & PROGRESSIVE DIAGRAM CHECK (Strict):\n"
                "   - Check if this diagram is mid-animation or only partially drawn (e.g. side by side comparison where one column is blank, or multi-step workflow missing later steps).\n"
                "   - If incomplete, select 'RETRY_OFFSET' and provide 'suggested_timestamp_sec' (15 to 45 seconds ahead).\n"
                "   - Only select 'KEEP' if fully assembled with all comparison columns and components present.\n"
                "3. IN-TEXT PLACEMENT: If KEEP, provide 'target_section' (matching section title), 'related_concept' (matching bullet point), and 'slide_topic' (2-4 word subject).\n"
                "4. CAPTION: If KEEP, provide 'caption' with WHAT is shown and WHY.\n"
                "Return JSON with decision ('KEEP', 'DISCARD', 'RETRY_OFFSET'), reasoning, suggested_timestamp_sec, caption, target_section, related_concept, slide_topic, and is_fully_assembled."
            )

            vision_model = self.model_name if "gpt-4" in self.model_name else "gpt-4o-mini"

            response = await self.client.chat.completions.create(
                model=vision_model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/jpeg;base64,{b64_img}"},
                            },
                        ],
                    }
                ],
                response_format={"type": "json_object"},
            )

            raw = response.choices[0].message.content or "{}"
            data = json.loads(clean_json_text(raw))
            return VLMFrameDecision(**data)
        except Exception:
            return VLMFrameDecision(
                decision="KEEP",
                reasoning="Fallback acceptance on vision error",
                caption=f"Visual slide captured at {timestamp_sec}s",
            )

    async def evaluate_frame(self, image_path: str) -> Dict[str, Any]:
        """Backward compatible evaluation returning is_informative boolean."""
        decision = await self.evaluate_frame_vlm(image_path=image_path, timestamp_sec=0)
        return {
            "is_informative": decision.decision != "DISCARD",
            "summary": decision.reasoning,
            "decision": decision.decision,
            "caption": decision.caption,
            "suggested_timestamp_sec": decision.suggested_timestamp_sec,
        }

