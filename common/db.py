"""Async database engine, session factory, and table initialization."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from common.config import get_settings
from common.models import Base

_engine: Optional[AsyncEngine] = None
_session_maker: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine(database_url: Optional[str] = None) -> AsyncEngine:
    """Provides singleton async engine, initializing if necessary."""
    global _engine
    if _engine is None or database_url is not None:
        url = database_url or get_settings().database_url
        engine_kwargs = {
            "echo": False,
            "future": True,
        }
        if "sqlite" in url:
            from sqlalchemy.pool import StaticPool
            engine_kwargs["connect_args"] = {"check_same_thread": False}
            engine_kwargs["poolclass"] = StaticPool
        else:
            engine_kwargs["pool_pre_ping"] = True

        engine = create_async_engine(url, **engine_kwargs)
        if database_url is None:
            _engine = engine
        return engine
    return _engine


def get_session_maker(
    database_url: Optional[str] = None,
) -> async_sessionmaker[AsyncSession]:
    """Provides async session maker for creating database sessions."""
    global _session_maker
    if _session_maker is None or database_url is not None:
        engine = get_engine(database_url)
        session_maker = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
        if database_url is None:
            _session_maker = session_maker
        return session_maker
    return _session_maker


async def init_db(engine: Optional[AsyncEngine] = None) -> None:
    """Creates all defined database tables asynchronously."""
    active_engine = engine or get_engine()
    async with active_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def get_db_session(
    database_url: Optional[str] = None,
) -> AsyncGenerator[AsyncSession, None]:
    """Context manager yielding an async session with auto commit and rollback."""
    session_factory = get_session_maker(database_url)
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
