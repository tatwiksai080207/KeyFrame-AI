"""Pluggable LLM factory and adapter exports."""

from typing import Optional
from common.config import get_settings
from worker.llm.base import (
    BaseLLMAdapter,
    SlideKeyMoment,
    SummarySection,
    SynthesisResult,
)
from worker.llm.gemini import GeminiAdapter
from worker.llm.openai import OpenAIAdapter


def get_llm_adapter(provider: Optional[str] = None) -> BaseLLMAdapter:
    """Instantiates the configured LLM adapter based on provider name or environment."""
    selected_provider = (provider or get_settings().llm_provider).lower()

    if selected_provider == "gemini":
        return GeminiAdapter()
    elif selected_provider == "openai":
        return OpenAIAdapter()
    else:
        raise ValueError(
            f"Unsupported LLM provider '{selected_provider}'. Allowed options are 'gemini' and 'openai'."
        )


__all__ = [
    "BaseLLMAdapter",
    "GeminiAdapter",
    "OpenAIAdapter",
    "SlideKeyMoment",
    "SummarySection",
    "SynthesisResult",
    "get_llm_adapter",
]

