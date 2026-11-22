"""Google Gemini multimodal synthesis adapter implementation."""

import asyncio
import json
import logging
import os
import re
import warnings
from typing import Any, Dict, List, Optional
from common.config import get_settings
from worker.llm.base import (
    SYNTHESIS_SYSTEM_INSTRUCTION,
    BaseLLMAdapter,
    SynthesisResult,
    VLMFrameDecision,
)

logger = logging.getLogger(__name__)

# Squelch Google SDK deprecation notice for legacy package
warnings.filterwarnings("ignore", category=FutureWarning, module="google.generativeai")


def clean_json_text(raw_text: str) -> str:
    """Extracts valid JSON string from potential markdown code fences."""
    cleaned = raw_text.strip()
    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", cleaned)
    if match:
        return match.group(1).strip()
    return cleaned


class GeminiAdapter(BaseLLMAdapter):
    """Google Gemini LLM synthesis adapter utilizing structured outputs."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> None:
        settings = get_settings()
        self.api_key = api_key or settings.gemini_api_key
        if not self.api_key:
            raise ValueError(
                "Gemini API key is required. Set GEMINI_API_KEY in your environment or .env file."
            )
        self.model_name = model_name or settings.llm_model

        import google.generativeai as genai

        genai.configure(api_key=self.api_key, transport="rest")
        self.client = genai.GenerativeModel(
            model_name=self.model_name,
            system_instruction=SYNTHESIS_SYSTEM_INSTRUCTION,
            generation_config={"response_mime_type": "application/json"},
        )

    async def synthesize(
        self,
        transcript_text: str,
        video_title: Optional[str] = None,
        duration_sec: Optional[int] = None,
        video_description: Optional[str] = None,
    ) -> SynthesisResult:
        """Invokes Gemini model asynchronously to synthesize study notes and keyframes."""
        duration_info = f"\nVideo Duration: {duration_sec} seconds" if duration_sec else ""
        desc_info = f"\nVideo Description:\n{video_description}\n" if video_description else ""
        prompt = (
            f"Video Title: {video_title or 'Untitled Video'}{duration_info}\n"
            f"{desc_info}\n"
            f"Timestamped Transcript Content:\n{transcript_text}\n\n"
            f"Reminder: Each timestamp_sec must be an exact integer seconds value taken directly from the [{'{seconds}'}s] markers and must not exceed the video duration. Never output concatenated minute and second digits."
        )

        loop = asyncio.get_running_loop()

        def _call_model():
            import google.generativeai as genai

            models_to_try = [self.model_name]
            for fb in ["gemini-3.5-flash-lite", "gemini-flash-lite-latest"]:
                if fb not in models_to_try:
                    models_to_try.append(fb)

            last_exc = None
            for candidate_model in models_to_try:
                try:
                    active_client = (
                        self.client
                        if candidate_model == self.model_name
                        else genai.GenerativeModel(
                            model_name=candidate_model,
                            system_instruction=SYNTHESIS_SYSTEM_INSTRUCTION,
                            generation_config={"response_mime_type": "application/json"},
                        )
                    )
                    return active_client.generate_content(prompt)
                except Exception as exc:
                    last_exc = exc
                    exc_msg = str(exc).lower()
                    if "429" in exc_msg or "quota" in exc_msg or "resource_exhausted" in exc_msg:
                        logger.warning(
                            f"Model '{candidate_model}' quota limit reached. Falling back to alternative model..."
                        )
                        continue
                    raise exc
            raise last_exc

        response = await loop.run_in_executor(None, _call_model)
        if not response or not response.text:
            raise ValueError("Empty response received from Gemini API.")

        raw_json = clean_json_text(response.text)
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
        """Uses Gemini VLM to judge slide value, completeness, offset suggestion, caption, and in-text placement."""
        if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
            return VLMFrameDecision(decision="DISCARD", reasoning="Empty or missing image file.")

        loop = asyncio.get_running_loop()

        def _sync_vlm_evaluate() -> VLMFrameDecision:
            import google.generativeai as genai

            with open(image_path, "rb") as f:
                img_data = f.read()

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
                "You are an expert Vision Language Model (VLM) curating technical slides and architecture diagrams for student study notes.\n\n"
                "Perform a strict and rigorous evaluation of this visual frame:\n"
                "1. STRICT INFORMATION TEST (Must be rigorously applied):\n"
                "   - Ask yourself: Does this image provide genuine, standalone educational or technical value that cannot be conveyed by text alone?\n"
                "   - If this image is just a presenter talking head webcam, generic background, intro/outro title card, logo, sponsor banner, or a simple bullet list with only 2-3 words that adds no visual insight, you MUST select 'DISCARD'.\n"
                "   - Only select 'KEEP' if the image contains a substantial visual artifact: an architectural schema, data flow diagram, sequence chart, network topology, code snippet, configuration file, comparison matrix, or software UI walkthrough.\n"
                "   - If the image is not strictly necessary or useful for a student reviewing these notes, DO NOT keep it.\n"
                "2. COMPLETENESS & PROGRESSIVE DIAGRAM CHECK (Strict):\n"
                "   - Technical and educational videos frequently build or reveal diagrams element by element (e.g. side by side comparisons, multi-step workflows, or architectures where blocks appear sequentially).\n"
                "   - Check if this diagram is currently incomplete:\n"
                "     a) SIDE BY SIDE OR MULTI COLUMN COMPARISONS: If the slide has headers for two or more options/columns (e.g., 'Option A vs Option B', 'Type 1 vs Type 2', 'Without vs With', 'Before vs After') and one column or half of the canvas is completely blank or missing content because the presenter has not drawn or revealed it yet, this frame is INCOMPLETE. You MUST NOT select 'KEEP'.\n"
                "     b) STEP BY STEP WORKFLOWS: If only the first 1 or 2 steps appear with empty canvas where remaining steps clearly follow, it is INCOMPLETE.\n"
                "     c) If more than 40% of the slide canvas is empty space where subsequent diagram components will obviously be placed, it is INCOMPLETE.\n"
                "   - For any INCOMPLETE frame:\n"
                "     * Select 'RETRY_OFFSET' and provide 'suggested_timestamp_sec' by estimating where the speaker finishes revealing the entire visual (typically 15 to 45 seconds later based on the surrounding transcript).\n"
                "     * If you believe the complete version is already captured elsewhere or cannot be recovered, select 'DISCARD'.\n"
                "   - Only select 'KEEP' if the visual diagram is in its FULLY ASSEMBLED state with all columns, components, and comparison sides present.\n"
                "3. IN-TEXT PLACEMENT (if KEEP):\n"
                "   - Which section title from the notes does this diagram belong to? (Set 'target_section')\n"
                "   - Which specific concept or bullet point does this visual directly illustrate? (Set 'related_concept' to help place it right next to that text)\n"
                "   - Provide 'slide_topic': A short 2 to 4 word subject or header identifying the diagram.\n"
                "4. CONSTRAINT B CAPTION (if KEEP):\n"
                "   - Provide an explanatory caption with WHAT diagram is shown and WHY it is important.\n\n"
                "Return strictly valid JSON:\n"
                "{\n"
                '  "decision": "KEEP | DISCARD | RETRY_OFFSET",\n'
                '  "reasoning": "Rigorous justification of why this image is or is not educationally essential",\n'
                '  "suggested_timestamp_sec": 145,\n'
                '  "caption": "WHAT: [Diagram description]. WHY: [Educational significance]",\n'
                '  "target_section": "Exact or closest matching section title",\n'
                '  "related_concept": "The specific bullet point or concept this visual illustrates",\n'
                '  "slide_topic": "Concise diagram topic",\n'
                '  "is_fully_assembled": true\n'
                "}"
            )

            models_to_try = [self.model_name]
            for fb in ["gemini-3.5-flash-lite", "gemini-flash-lite-latest"]:
                if fb not in models_to_try:
                    models_to_try.append(fb)

            for candidate_model in models_to_try:
                try:
                    active_client = (
                        self.client
                        if candidate_model == self.model_name
                        else genai.GenerativeModel(
                            model_name=candidate_model,
                            generation_config={"response_mime_type": "application/json"},
                        )
                    )
                    res = active_client.generate_content(
                        [{"mime_type": "image/jpeg", "data": img_data}, prompt],
                        generation_config={"response_mime_type": "application/json"},
                    )
                    if res and res.text:
                        parsed = json.loads(clean_json_text(res.text))
                        return VLMFrameDecision(
                            decision=parsed.get("decision", "KEEP"),
                            reasoning=parsed.get("reasoning", ""),
                            suggested_timestamp_sec=parsed.get("suggested_timestamp_sec"),
                            caption=parsed.get("caption", ""),
                            target_section=parsed.get("target_section"),
                            related_concept=parsed.get("related_concept"),
                            slide_topic=parsed.get("slide_topic"),
                            is_fully_assembled=parsed.get("is_fully_assembled", True),
                        )
                except Exception as exc:
                    exc_msg = str(exc).lower()
                    if "429" in exc_msg or "quota" in exc_msg or "resource_exhausted" in exc_msg:
                        logger.warning(
                            f"Vision model '{candidate_model}' quota limit reached. Falling back to alternative model..."
                        )
                        continue
                    logger.warning(f"VLM model error with {candidate_model}: {exc}")

            return VLMFrameDecision(
                decision="KEEP",
                reasoning="Fallback acceptance on vision timeout",
                caption=f"Visual slide captured at {timestamp_sec}s",
            )

        try:
            return await loop.run_in_executor(None, _sync_vlm_evaluate)
        except Exception as e:
            logger.warning(f"Vision evaluation failed for frame {image_path}: {e}")
            return VLMFrameDecision(
                decision="KEEP",
                reasoning=f"Evaluation error: {e}",
                caption=f"Visual slide captured at {timestamp_sec}s",
            )

    async def evaluate_frame(self, image_path: str) -> Dict[str, Any]:
        """Backward compatible evaluation returning is_informative boolean and summary."""
        decision = await self.evaluate_frame_vlm(image_path=image_path, timestamp_sec=0)
        return {
            "is_informative": decision.decision != "DISCARD",
            "summary": decision.reasoning,
            "decision": decision.decision,
            "caption": decision.caption,
            "suggested_timestamp_sec": decision.suggested_timestamp_sec,
        }
