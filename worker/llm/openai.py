"""OpenAI synthesis adapter implementation."""

import json
from typing import Optional
from common.config import get_settings
from worker.llm.base import (
    SYNTHESIS_SYSTEM_INSTRUCTION,
    BaseLLMAdapter,
    SynthesisResult,
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
    ) -> SynthesisResult:
        """Invokes OpenAI model asynchronously to synthesize study notes and keyframes."""
        prompt = (
            f"Video Title: {video_title or 'Untitled Video'}\n\n"
            f"Timestamped Transcript Content:\n{transcript_text}"
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

