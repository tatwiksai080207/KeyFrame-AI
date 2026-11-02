"""Google Gemini multimodal synthesis adapter implementation."""

import asyncio
import json
import re
import warnings
from typing import Optional
from common.config import get_settings
from worker.llm.base import (
    SYNTHESIS_SYSTEM_INSTRUCTION,
    BaseLLMAdapter,
    SynthesisResult,
)

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

        genai.configure(api_key=self.api_key)
        self.client = genai.GenerativeModel(
            model_name=self.model_name,
            system_instruction=SYNTHESIS_SYSTEM_INSTRUCTION,
            generation_config={"response_mime_type": "application/json"},
        )

    async def synthesize(
        self,
        transcript_text: str,
        video_title: Optional[str] = None,
    ) -> SynthesisResult:
        """Invokes Gemini model asynchronously to synthesize study notes and keyframes."""
        prompt = (
            f"Video Title: {video_title or 'Untitled Video'}\n\n"
            f"Timestamped Transcript Content:\n{transcript_text}"
        )

        loop = asyncio.get_running_loop()

        def _call_model():
            return self.client.generate_content(prompt)

        response = await loop.run_in_executor(None, _call_model)
        if not response or not response.text:
            raise ValueError("Empty response received from Gemini API.")

        raw_json = clean_json_text(response.text)
        data = json.loads(raw_json)
        return SynthesisResult(**data)
