"""Google Gemini multimodal synthesis adapter implementation."""

import asyncio
import json
import logging
import os
import re
import warnings
from typing import Any, Dict, Optional
from common.config import get_settings
from worker.llm.base import (
    SYNTHESIS_SYSTEM_INSTRUCTION,
    BaseLLMAdapter,
    SynthesisResult,
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

    async def evaluate_frame(self, image_path: str) -> Dict[str, Any]:
        """Evaluates whether an extracted video frame contains an informative slide or diagram vs talking head face."""
        if not os.path.exists(image_path) or os.path.getsize(image_path) == 0:
            return {"is_informative": False, "reason": "Empty or missing image"}

        loop = asyncio.get_running_loop()

        def _sync_evaluate():
            import google.generativeai as genai

            with open(image_path, "rb") as f:
                img_data = f.read()

            prompt = (
                "Evaluate whether this video frame shows an informative technical slide, code, architecture diagram, "
                "whiteboard, chart, or technical UI vs a generic camera talking head face, sponsor ad, or empty scene. "
                "Respond in JSON with keys: is_informative (boolean), content_type (string), summary (string)."
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
                        return json.loads(clean_json_text(res.text))
                except Exception as exc:
                    exc_msg = str(exc).lower()
                    if "429" in exc_msg or "quota" in exc_msg or "resource_exhausted" in exc_msg:
                        logger.warning(
                            f"Vision model '{candidate_model}' quota limit reached. Falling back to alternative model..."
                        )
                        continue
                    raise exc
            return {"is_informative": True, "reason": "Empty vision response"}

        try:
            return await loop.run_in_executor(None, _sync_evaluate)
        except Exception as e:
            logger.warning(f"Vision evaluation failed for frame {image_path}: {e}")
            return {"is_informative": True, "reason": f"Evaluation error: {e}"}
