import os
from collections.abc import AsyncIterator, Iterator

import aio_pika
import pytest
import pytest_asyncio
from alembic import command
from dotenv import load_dotenv
from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

import models  # noqa: F401
from core.config import Settings, get_settings
from db import session as database_session
from db.base import Base
from db.session import AsyncSession, build_async_engine
from messaging.broker import build_broker
from messaging.topology import PaymentsTopology
from tests.support.api import TEST_API_KEY
from tests.support.database import (
    alembic_config,
    build_test_database_url,
    database_name,
    ensure_test_database,
    postgres_server_available,
)
from tests.support.rabbitmq import (
    TEST_RETRY_BASE_DELAY_MS,
    TEST_VHOST,
    create_vhost,
    delete_vhost,
    rabbitmq_available,
)

load_dotenv(override=False)
os.environ["API_KEY"] = TEST_API_KEY
os.environ["WEBHOOK_ALLOW_PRIVATE"] = "false"
get_settings.cache_clear()
BASE_DATABASE_URL = Settings().database_url
DATABASE_URL = build_test_database_url(BASE_DATABASE_URL)


def postgres_available() -> bool:
    return postgres_server_available(BASE_DATABASE_URL)


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    del config
    unavailable = {
        "database": None if postgres_available() else "PostgreSQL is unavailable",
        "rabbitmq": None
        if rabbitmq_available(Settings())
        else "RabbitMQ management API is unavailable",
    }
    for marker, reason in unavailable.items():
        if reason is None:
            continue
        for item in items:
            if item.get_closest_marker(marker) is not None:
                item.add_marker(pytest.mark.skip(reason=reason))


@pytest.fixture(scope="session", autouse=True)
def migrated_schema() -> Iterator[None]:
    if not postgres_available():
        yield
        return

    ensure_test_database(BASE_DATABASE_URL)
    get_settings.cache_clear()
    config = alembic_config(DATABASE_URL)
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


@pytest_asyncio.fixture(autouse=True)
async def fresh_application_pool() -> AsyncIterator[None]:
    yield
    await database_session.engine.dispose()


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
    tables = ", ".join(table.name for table in Base.metadata.sorted_tables)
    async with session_factory() as database_session:
        await database_session.execute(text(f"TRUNCATE TABLE {tables}"))
        await database_session.commit()
        yield database_session
        await database_session.rollback()


@pytest.fixture(scope="session")
def rabbitmq_settings() -> Iterator[Settings]:
    settings = Settings(
        RABBITMQ_VHOST=TEST_VHOST, RETRY_BASE_DELAY_MS=TEST_RETRY_BASE_DELAY_MS
    )
    delete_vhost(settings, TEST_VHOST)
    create_vhost(settings, TEST_VHOST)
    yield settings
    delete_vhost(settings, TEST_VHOST)


@pytest_asyncio.fixture
async def amqp_channel(
    rabbitmq_settings: Settings,
) -> AsyncIterator[aio_pika.abc.AbstractChannel]:
    connection = await aio_pika.connect_robust(rabbitmq_settings.rabbitmq_url)
    async with connection:
        yield await connection.channel()


@pytest_asyncio.fixture
async def broker(
    rabbitmq_settings: Settings, amqp_channel: aio_pika.abc.AbstractChannel
) -> AsyncIterator[RabbitBroker]:
    rabbit_broker = build_broker(rabbitmq_settings)
    await rabbit_broker.connect()
    topology = PaymentsTopology(rabbitmq_settings)
    await topology.declare(rabbit_broker)
    for queue in topology.queues:
        declared = await amqp_channel.get_queue(queue.name)
        await declared.purge()
    yield rabbit_broker
    await rabbit_broker.stop()
