"""Application configuration loaded from environment variables and .env file."""

import json
from functools import lru_cache
from typing import List, Literal, Optional, Union
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration settings for KeyFrame-AI worker and pipeline."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # RabbitMQ Broker Configuration
    rabbitmq_url: str = Field(
        default="amqp://guest:guest@localhost:5672/",
        validation_alias="RABBITMQ_URL",
    )

    # Database Configuration
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/notes_agent",
        validation_alias="DATABASE_URL",
    )

    # Redis Configuration
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias="REDIS_URL",
    )

    # LLM Settings
    llm_provider: Literal["gemini", "openai"] = Field(
        default="gemini",
        validation_alias="LLM_PROVIDER",
    )
    gemini_api_key: Optional[str] = Field(
        default=None,
        validation_alias="GEMINI_API_KEY",
    )
    openai_api_key: Optional[str] = Field(
        default=None,
        validation_alias="OPENAI_API_KEY",
    )
    llm_model: str = Field(
        default="gemini-3.5-flash-lite",
        validation_alias="LLM_MODEL",
    )
    llm_rate_limit_rpm: int = Field(
        default=15,
        validation_alias="LLM_RATE_LIMIT_RPM",
    )

    # Notion Settings
    notion_oauth_client_id: Optional[str] = Field(
        default=None,
        validation_alias="NOTION_OAUTH_CLIENT_ID",
    )
    notion_oauth_client_secret: Optional[str] = Field(
        default=None,
        validation_alias="NOTION_OAUTH_CLIENT_SECRET",
    )
    notion_default_parent_page_id: Optional[str] = Field(
        default=None,
        validation_alias="NOTION_DEFAULT_PARENT_PAGE_ID",
    )

    # Frame Media Directory
    frame_storage_dir: str = Field(
        default="./frames",
        validation_alias="FRAME_STORAGE_DIR",
    )
    public_base_url: Optional[str] = Field(
        default=None,
        validation_alias="PUBLIC_BASE_URL",
    )

    # Constraints and Fault Tolerance
    max_video_duration_sec: int = Field(
        default=1200,  # 20 minutes strictly per Constraint A
        validation_alias="MAX_VIDEO_DURATION_SEC",
    )
    max_retry_attempts: int = Field(
        default=5,
        validation_alias="MAX_RETRY_ATTEMPTS",
    )
    retry_backoff_schedule: Union[List[int], str] = Field(
        default=[10, 30, 90],
        validation_alias="RETRY_BACKOFF_SCHEDULE",
    )

    @field_validator("retry_backoff_schedule", mode="after")
    @classmethod
    def parse_backoff_schedule(cls, value: Union[List[int], str]) -> List[int]:
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                try:
                    return json.loads(value)
                except Exception:
                    pass
            return [int(x.strip()) for x in value.strip("[]").split(",") if x.strip()]
        return value


@lru_cache()
def get_settings() -> Settings:
    """Returns cached singleton configuration instance."""
    return Settings()
