from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession as _AsyncSession

from core.config import Settings, get_settings
from core.custom_logging import get_logger

logger = get_logger()
_settings = get_settings()


def build_async_engine(settings: Settings | None = None) -> AsyncEngine:
    resolved = settings or _settings
    return create_async_engine(
        url=resolved.database_url,
        echo=resolved.db_echo,
        future=True,
        pool_size=resolved.db_pool_size,
        max_overflow=resolved.db_max_overflow,
        pool_timeout=resolved.db_pool_timeout,
        pool_recycle=resolved.db_pool_recycle,
        pool_pre_ping=resolved.db_pool_pre_ping,
    )


engine = build_async_engine()


class AsyncSession(_AsyncSession):
    async def commit(self) -> None:
        try:
            await super().commit()
        except SQLAlchemyError as e:
            await self.rollback()
            logger.error("Commit failed, transaction rolled back", exc_info=e)
            raise

    async def execute(self, *args, **kwargs):
        try:
            return await super().execute(*args, **kwargs)
        except SQLAlchemyError as e:
            await self.rollback()
            logger.error("Execute failed, transaction rolled back", exc_info=e)
            raise

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["AsyncSession"]:
        try:
            yield self
            await self.commit()
        except Exception as e:
            await self.rollback()
            logger.error("Transaction failed, rolled back", exc_info=e)
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
            await conn.execute(text("SELECT 1"))
        logger.info("Connected to PostgreSQL")
        return True
    except (SQLAlchemyError, OSError) as e:
        logger.critical("PostgreSQL is not available")
        logger.debug(f"Raw DB connection error: {e}")
        return False
