"""Tests for application settings and configuration loader."""

import os
from common.config import Settings, get_settings


def test_default_settings():
    """Verify default settings reflect project constraints and broker specs."""
    settings = Settings()
    assert settings.max_video_duration_sec == 1200
    assert settings.max_retry_attempts == 5
    assert settings.retry_backoff_schedule == [10, 30, 90]
    assert settings.llm_rate_limit_rpm == 15
    assert settings.llm_provider in ["gemini", "openai"]


def test_env_override(monkeypatch):
    """Verify settings parse overrides from environment variables."""
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("MAX_VIDEO_DURATION_SEC", "900")
    monkeypatch.setenv("RETRY_BACKOFF_SCHEDULE", "[5, 15, 45]")

    settings = Settings()
    assert settings.llm_provider == "openai"
    assert settings.max_video_duration_sec == 900
    assert settings.retry_backoff_schedule == [5, 15, 45]


def test_comma_separated_backoff_schedule(monkeypatch):
    """Verify string comma-separated list parser works for retry schedule."""
    monkeypatch.setenv("RETRY_BACKOFF_SCHEDULE", "15, 45, 120")
    settings = Settings()
    assert settings.retry_backoff_schedule == [15, 45, 120]


def test_get_settings_cached():
    """Verify get_settings returns a valid singleton."""
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2

