from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator

ALLOWED_YOUTUBE_HOSTS = {"www.youtube.com", "youtube.com", "m.youtube.com", "youtu.be"}

class JobCreateRequest(BaseModel):

    youtube_url: str = Field(
        ...,
        description="Full YouTube video URL",
        examples=["https://www.youtube.com/watch?v=dQw4w9WgXcQ"],
    )
    notion_token: str = Field(..., min_length=1, description="Notion OAuth / integration token")
    notion_parent_id: Optional[str] = Field(
        default=None, description="Notion parent page / database ID"
    )

    @field_validator("youtube_url")
    @classmethod
    def validate_youtube_url(cls, value: str) -> str:
        value = value.strip()
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or parsed.hostname not in ALLOWED_YOUTUBE_HOSTS:
            raise ValueError("Invalid YouTube URL. Expected a youtube.com/watch or youtu.be link.")
        return value


class JobCreateResponse(BaseModel):
    job_id: str
    status: str = "QUEUED"
    video_id: str

class JobStatusResponse(BaseModel):
    # GET /jobs/{job_id}
    job_id: str
    status: str
    progress: int = Field(ge=0, le=100)
    video_title: Optional[str] = None
    duration_sec: Optional[int] = None
    warnings: List[Dict[str, Any]] = Field(default_factory=list)
    error: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None


class JobResultResponse(BaseModel):
    # GET /jobs/{job_id}/results 
    status: str
    result: Dict[str, Any]
    warnings: List[Dict[str, Any]] = Field(default_factory=list)