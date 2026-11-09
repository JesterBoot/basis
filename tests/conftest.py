from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from dotenv import load_dotenv
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from core.config import Settings, get_settings
from db import session as database_session
from db.session import AsyncSession, build_async_engine
from tests.support.database import (
    build_test_database_url,
    database_name,
    ensure_test_database,
    postgres_server_available,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(override=False)
BASE_DATABASE_URL = Settings().database_url
DATABASE_URL = build_test_database_url(BASE_DATABASE_URL)


def _sync_database_url() -> str:
    return DATABASE_URL


def postgres_available() -> bool:
    return postgres_server_available(BASE_DATABASE_URL)


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    del config
    if postgres_available():
        return

    skip_database = pytest.mark.skip(
        reason="PostgreSQL is unavailable",
    )
    for item in items:
        if item.get_closest_marker("database") is not None:
            item.add_marker(skip_database)


def _alembic_config() -> Config:
    config = Config(str(PROJECT_ROOT / "src" / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", DATABASE_URL.replace("%", "%%"))
    return config


@pytest.fixture(scope="session", autouse=True)
def migrated_schema() -> Iterator[None]:
    if not postgres_available():
        yield
        return

    ensure_test_database(BASE_DATABASE_URL)
    get_settings.cache_clear()
    config = _alembic_config()
    try:
        command.upgrade(config, "head")
        yield
    finally:
        command.downgrade(config, "base")
        get_settings.cache_clear()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def application_database(migrated_schema: None) -> AsyncIterator[None]:
    if not postgres_available():
        yield
        return

    settings = Settings(_env_file=None, database_url_raw=DATABASE_URL)
    test_engine = build_async_engine(settings)
    expected_database = database_name(DATABASE_URL)
    async with test_engine.connect() as connection:
        actual_database = await connection.scalar(select(func.current_database()))
    if actual_database != expected_database:
        await test_engine.dispose()
        raise RuntimeError(
            "Pytest connected to an unexpected database: "
            f"expected {expected_database!r}, got {actual_database!r}"
        )

    original_engine = database_session.engine
    original_session_factory = database_session.async_session_factory
    database_session.engine = test_engine
    database_session.async_session_factory = async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    try:
        yield
    finally:
        database_session.engine = original_engine
        database_session.async_session_factory = original_session_factory
        await test_engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def async_engine(application_database: None) -> AsyncIterator[AsyncEngine]:
    del application_database
    settings = Settings(_env_file=None, database_url_raw=DATABASE_URL)
    engine = build_async_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(
    async_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


@pytest_asyncio.fixture
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as database_session:
        await database_session.commit()
        yield database_session
        await database_session.rollback()
