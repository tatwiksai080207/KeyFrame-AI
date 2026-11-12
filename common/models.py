"""Declarative database models and state machine definitions for jobs."""

import enum
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    JSON,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all declarative models."""
    pass


class JobStatus(str, enum.Enum):
    """Allowed job lifecycle status values per project specification."""

    QUEUED = "QUEUED"
    TRANSCRIBING = "TRANSCRIBING"
    SUMMARIZING = "SUMMARIZING"
    EXTRACTING_FRAMES = "EXTRACTING_FRAMES"
    PUBLISHING = "PUBLISHING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_WARNINGS = "COMPLETED_WITH_WARNINGS"
    FAILED = "FAILED"
    DEAD_LETTERED = "DEAD_LETTERED"

    @classmethod
    def terminal_statuses(cls) -> Set["JobStatus"]:
        """Returns set of terminal states."""
        return {
            cls.COMPLETED,
            cls.COMPLETED_WITH_WARNINGS,
            cls.FAILED,
            cls.DEAD_LETTERED,
        }

    @property
    def is_terminal(self) -> bool:
        """Checks if current state is terminal."""
        return self in self.terminal_statuses()


# Valid forward state transitions
ALLOWED_TRANSITIONS: Dict[JobStatus, Set[JobStatus]] = {
    JobStatus.QUEUED: {
        JobStatus.TRANSCRIBING,
        JobStatus.FAILED,
        JobStatus.DEAD_LETTERED,
    },
    JobStatus.TRANSCRIBING: {
        JobStatus.SUMMARIZING,
        JobStatus.FAILED,
        JobStatus.DEAD_LETTERED,
    },
    JobStatus.SUMMARIZING: {
        JobStatus.EXTRACTING_FRAMES,
        JobStatus.FAILED,
        JobStatus.DEAD_LETTERED,
    },
    JobStatus.EXTRACTING_FRAMES: {
        JobStatus.PUBLISHING,
        JobStatus.FAILED,
        JobStatus.DEAD_LETTERED,
    },
    JobStatus.PUBLISHING: {
        JobStatus.COMPLETED,
        JobStatus.COMPLETED_WITH_WARNINGS,
        JobStatus.FAILED,
        JobStatus.DEAD_LETTERED,
    },
    JobStatus.COMPLETED: set(),
    JobStatus.COMPLETED_WITH_WARNINGS: set(),
    JobStatus.FAILED: set(),
    JobStatus.DEAD_LETTERED: set(),
}


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    """Validates if transition from current to target status is permitted."""
    if current == target:
        return True
    return target in ALLOWED_TRANSITIONS.get(current, set())


def default_progress_for_status(status: JobStatus) -> int:
    """Returns baseline progress percentage for a given status."""
    mapping = {
        JobStatus.QUEUED: 0,
        JobStatus.TRANSCRIBING: 25,
        JobStatus.SUMMARIZING: 50,
        JobStatus.EXTRACTING_FRAMES: 75,
        JobStatus.PUBLISHING: 90,
        JobStatus.COMPLETED: 100,
        JobStatus.COMPLETED_WITH_WARNINGS: 100,
        JobStatus.FAILED: 0,
        JobStatus.DEAD_LETTERED: 0,
    }
    return mapping.get(status, 0)


class Job(Base):
    """Job persistence entity tracking distributed pipeline progress and outputs."""

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )
    video_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    youtube_url: Mapped[str] = mapped_column(String(512), nullable=False)
    video_title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    video_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_sec: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    status: Mapped[str] = mapped_column(
        String(32),
        default=JobStatus.QUEUED.value,
        index=True,
        nullable=False,
    )
    progress: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    notion_token: Mapped[str] = mapped_column(Text, nullable=False)
    notion_parent_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    notion_page_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    raw_transcript: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    llm_summary: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    extracted_frames: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    warnings: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSON,
        default=list,
        nullable=False,
    )
    error: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __init__(
        self,
        video_id: str,
        youtube_url: str,
        notion_token: str,
        id: Optional[str] = None,
        video_title: Optional[str] = None,
        duration_sec: Optional[int] = None,
        status: str = JobStatus.QUEUED.value,
        progress: int = 0,
        notion_parent_id: Optional[str] = None,
        notion_page_url: Optional[str] = None,
        raw_transcript: Optional[Any] = None,
        llm_summary: Optional[Any] = None,
        extracted_frames: Optional[Any] = None,
        warnings: Optional[List[Dict[str, Any]]] = None,
        error: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.id = id or str(uuid.uuid4())
        self.video_id = video_id
        self.youtube_url = youtube_url
        self.notion_token = notion_token
        self.video_title = video_title
        self.duration_sec = duration_sec
        self.status = status
        self.progress = progress
        self.notion_parent_id = notion_parent_id
        self.notion_page_url = notion_page_url
        self.raw_transcript = raw_transcript
        self.llm_summary = llm_summary
        self.extracted_frames = extracted_frames
        self.warnings = warnings if warnings is not None else []
        self.error = error
        self.retry_count = retry_count

    def transition_to(
        self,
        new_status: JobStatus,
        progress: Optional[int] = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Safely mutates job status, validating state machine rules."""
        current_status = JobStatus(self.status)
        if not can_transition(current_status, new_status):
            raise ValueError(
                f"Illegal state transition from {current_status.value} to {new_status.value}"
            )
        self.status = new_status.value
        self.progress = (
            progress if progress is not None else default_progress_for_status(new_status)
        )
        if error:
            self.error = error
        self.updated_at = datetime.now(timezone.utc)

    def add_warning(self, code: str, message: str, timestamp_sec: Optional[int] = None) -> None:
        """Appends a non fatal warning to the warnings list."""
        warning_entry: Dict[str, Any] = {
            "code": code,
            "message": message,
        }
        if timestamp_sec is not None:
            warning_entry["timestamp_sec"] = timestamp_sec

        current_warnings = list(self.warnings or [])
        current_warnings.append(warning_entry)
        self.warnings = current_warnings
