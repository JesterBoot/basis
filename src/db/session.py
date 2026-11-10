from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession as _AsyncSession
from structlog import get_logger

from core.config import Settings, get_settings

logger = get_logger()
_settings = get_settings()


def _to_async_database_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def _build_engine_kwargs(settings: Settings | None = None) -> dict[str, object]:
    resolved_settings = settings or _settings
    database_url = _to_async_database_url(resolved_settings.database_url)
    kwargs: dict[str, object] = {
        "url": database_url,
        "echo": resolved_settings.db_echo,
        "future": True,
    }

    kwargs.update(
        {
            "pool_size": resolved_settings.db_pool_size,
            "max_overflow": resolved_settings.db_max_overflow,
            "pool_timeout": resolved_settings.db_pool_timeout,
            "pool_recycle": resolved_settings.db_pool_recycle,
            "pool_pre_ping": resolved_settings.db_pool_pre_ping,
        }
    )

    return kwargs


def build_async_engine(settings: Settings | None = None) -> AsyncEngine:
    return create_async_engine(**_build_engine_kwargs(settings))


engine = build_async_engine()


class AsyncSession(_AsyncSession):
    async def commit(self) -> None:
        try:
            await super().commit()
        except SQLAlchemyError as exc:
            await self.rollback()
            logger.error("Commit failed, transaction rolled back", exc_info=exc)
            raise

    async def execute(self, *args, **kwargs):
        try:
            return await super().execute(*args, **kwargs)
        except SQLAlchemyError as exc:
            await self.rollback()
            logger.error("Execute failed, transaction rolled back", exc_info=exc)
            raise

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["AsyncSession"]:
        try:
            yield self
            await self.commit()
        except Exception as exc:
            await self.rollback()
            logger.error("Transaction failed, rolled back", exc_info=exc)
            raise


async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with async_session_factory() as session:
        yield session


@asynccontextmanager
async def tasks_session_context() -> AsyncIterator[AsyncSession]:
    async with async_session_factory() as session:
        yield session


async def check_conn_psql() -> bool:
    try:
        async with engine.begin() as conn:
            await conn.execute(select(1))
        logger.info("Connected to PostgreSQL/SQL database")
        return True
    except Exception as exc:
        logger.critical("Database is not available", exc_info=exc)
        return False
