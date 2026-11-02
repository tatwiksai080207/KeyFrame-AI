"""Tests for pluggable multimodal LLM adapters and Constraint B caption validation."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import ValidationError

from worker.llm.base import (
    SlideKeyMoment,
    SummarySection,
    SynthesisResult,
)
from worker.llm.gemini import GeminiAdapter, clean_json_text
from worker.llm.openai import OpenAIAdapter
from worker.llm import get_llm_adapter


def test_schema_constraint_b_caption_enforcement():
    """Verify SlideKeyMoment strictly requires non-empty caption per Constraint B."""
    valid_moment = SlideKeyMoment(
        timestamp_sec=120,
        caption="Architecture diagram showing RabbitMQ broker and worker pools.",
    )
    assert valid_moment.timestamp_sec == 120
    assert "RabbitMQ" in valid_moment.caption

    # Empty caption must fail validation
    with pytest.raises(ValidationError):
        SlideKeyMoment(timestamp_sec=120, caption="")

    # Whitespace caption must fail validation
    with pytest.raises(ValidationError):
        SlideKeyMoment(timestamp_sec=120, caption="     ")


def test_synthesis_result_validation():
    """Verify SynthesisResult structure with sections and key moments."""
    res = SynthesisResult(
        overview="Comprehensive overview of distributed systems.",
        sections=[
            SummarySection(
                section_title="Message Brokers",
                bullet_points=["Provides durable queuing", "Guarantees at least once delivery"],
            )
        ],
        key_moments=[
            SlideKeyMoment(
                timestamp_sec=45,
                caption="Slide summarizing AMQP message lifecycle and acknowledgment states.",
            )
        ],
    )
    assert len(res.sections) == 1
    assert len(res.key_moments) == 1
    assert res.key_moments[0].timestamp_sec == 45


def test_clean_json_text():
    """Verify markdown code block fences are stripped cleanly."""
    raw = '```json\n{"overview": "Test overview"}\n```'
    assert clean_json_text(raw) == '{"overview": "Test overview"}'

    raw_plain = '{"overview": "Plain JSON"}'
    assert clean_json_text(raw_plain) == '{"overview": "Plain JSON"}'


def test_factory_provider_selection():
    """Verify factory returns appropriate adapter instance based on provider name."""
    with patch("worker.llm.gemini.get_settings") as mock_settings:
        mock_settings.return_value.gemini_api_key = "test_gemini_key"
        mock_settings.return_value.llm_model = "gemini-1.5-flash"
        adapter = get_llm_adapter("gemini")
        assert isinstance(adapter, GeminiAdapter)

    with patch("worker.llm.openai.get_settings") as mock_settings:
        mock_settings.return_value.openai_api_key = "test_openai_key"
        mock_settings.return_value.llm_model = "gpt-4o-mini"
        adapter = get_llm_adapter("openai")
        assert isinstance(adapter, OpenAIAdapter)

    with pytest.raises(ValueError):
        get_llm_adapter("unsupported_provider")


@pytest.mark.asyncio
async def test_gemini_adapter_synthesize_mock():
    """Verify GeminiAdapter synthesizes structured notes and parses JSON response."""
    sample_response_data = {
        "overview": "Lecture on asynchronous task queues.",
        "sections": [
            {
                "section_title": "Core Mechanics",
                "bullet_points": ["Workers pull tasks", "Prefetch set to 1"],
            }
        ],
        "key_moments": [
            {
                "timestamp_sec": 88,
                "caption": "Diagram illustrating consumer worker pool receiving messages.",
            }
        ],
    }

    mock_client = MagicMock()
    mock_client.generate_content.return_value.text = json.dumps(sample_response_data)

    adapter = GeminiAdapter(api_key="mock_key", model_name="gemini-1.5-flash")
    adapter.client = mock_client

    result = await adapter.synthesize(
        transcript_text="00:01 Welcome to task queues...",
        video_title="Task Queues 101",
    )

    assert isinstance(result, SynthesisResult)
    assert result.overview == "Lecture on asynchronous task queues."
    assert len(result.key_moments) == 1
    assert result.key_moments[0].timestamp_sec == 88
    assert "Diagram illustrating" in result.key_moments[0].caption


@pytest.mark.asyncio
async def test_openai_adapter_synthesize_mock():
    """Verify OpenAIAdapter synthesizes structured notes and parses JSON response."""
    sample_response_data = {
        "overview": "Overview of state machines in distributed pipelines.",
        "sections": [
            {
                "section_title": "Job Lifecycle",
                "bullet_points": ["Queued to Transcribing", "Dead letter on exhaustion"],
            }
        ],
        "key_moments": [
            {
                "timestamp_sec": 210,
                "caption": "State diagram of job transitions and retry backoff mechanics.",
            }
        ],
    }

    adapter = OpenAIAdapter(api_key="mock_key", model_name="gpt-4o-mini")
    mock_choice = MagicMock()
    mock_choice.message.content = json.dumps(sample_response_data)
    mock_response = MagicMock(choices=[mock_choice])

    adapter.client.chat.completions.create = MagicMock(return_value=mock_response)
    # Async mock for create
    adapter.client.chat.completions.create = AsyncMock(return_value=mock_response)

    result = await adapter.synthesize(
        transcript_text="00:10 State transitions...",
        video_title="State Machines",
    )

    assert isinstance(result, SynthesisResult)
    assert result.overview == "Overview of state machines in distributed pipelines."
    assert len(result.key_moments) == 1
    assert result.key_moments[0].timestamp_sec == 210
    assert "State diagram" in result.key_moments[0].caption
