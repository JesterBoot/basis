import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import (
    DataError,
    DBAPIError,
    IntegrityError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from core.config import Settings
from db.session import AsyncSession, build_async_engine, get_session
from models import OutboxEvent, Payment
from tests.support.api import (
    PAYMENT_BODY,
    PAYMENTS_URL,
    TEST_API_KEY,
    new_idempotency_key,
    post_payment,
)
from tests.support.database import count_rows


@pytest_asyncio.fixture
async def lenient_client(application: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        transport=ASGITransport(app=application, raise_app_exceptions=False),
        base_url="http://testserver",
        headers={"X-API-Key": TEST_API_KEY},
    ) as http_client:
        yield http_client


@pytest_asyncio.fixture
async def client_with_unreachable_database(
    application: FastAPI,
) -> AsyncIterator[AsyncClient]:
    settings = Settings(
        _env_file=None,
        DATABASE_URL="postgresql+psycopg://basis:basis@127.0.0.1:1/basis",
        db_pool_timeout=1,
    )
    engine = build_async_engine(settings)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def unreachable_session() -> AsyncIterator[AsyncSession]:
        async with factory() as session:
            yield session

    application.dependency_overrides[get_session] = unreachable_session
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://testserver",
        headers={"X-API-Key": TEST_API_KEY},
    ) as http_client:
        yield http_client
    await engine.dispose()


def _db_error(kind: str) -> Exception:
    origin = Exception("server closed the connection unexpectedly")
    errors: dict[str, Exception] = {
        "operational": OperationalError("INSERT", {}, origin),
        "interface": InterfaceError("INSERT", {}, origin),
        "pool-timeout": PoolTimeoutError("QueuePool limit reached"),
        "invalidated": DBAPIError("INSERT", {}, origin, connection_invalidated=True),
        "integrity": IntegrityError("INSERT", {}, origin),
        "data": DataError("INSERT", {}, origin),
    }
    return errors[kind]


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", PAYMENTS_URL), ("GET", f"{PAYMENTS_URL}/{uuid.uuid4()}")],
    ids=["create", "get"],
)
async def test_database_outage_returns_503(
    client_with_unreachable_database: AsyncClient, method: str, path: str
) -> None:
    response = await client_with_unreachable_database.request(
        method,
        path,
        json=PAYMENT_BODY,
        headers={"Idempotency-Key": new_idempotency_key()},
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert response.headers["Retry-After"] == "5"


@pytest.mark.database
@pytest.mark.parametrize(
    ("kind", "expected_status"),
    [
        ("operational", 503),
        ("interface", 503),
        ("pool-timeout", 503),
        ("invalidated", 503),
        ("integrity", 500),
        ("data", 500),
    ],
)
async def test_e_a14_only_availability_errors_map_to_503(
    application: FastAPI,
    lenient_client: AsyncClient,
    async_engine: AsyncEngine,
    kind: str,
    expected_status: int,
) -> None:
    class BrokenSession(AsyncSession):
        async def _fail(self, *args: Any, **kwargs: Any) -> Any:
            raise _db_error(kind)

        execute = scalar = scalars = get = flush = commit = _fail

    factory = async_sessionmaker(async_engine, class_=BrokenSession)

    async def broken_session() -> AsyncIterator[AsyncSession]:
        async with factory() as own_session:
            yield own_session

    application.dependency_overrides[get_session] = broken_session

    response = await post_payment(lenient_client, PAYMENT_BODY, new_idempotency_key())

    assert response.status_code == expected_status


@pytest.mark.database
async def test_e_a14_commit_failure_leaves_no_payment_or_outbox_row(
    application: FastAPI,
    lenient_client: AsyncClient,
    session: AsyncSession,
    async_engine: AsyncEngine,
) -> None:
    class CommitFailsSession(AsyncSession):
        async def commit(self) -> None:
            raise OperationalError("COMMIT", {}, Exception("connection lost"))

    factory = async_sessionmaker(async_engine, class_=CommitFailsSession)

    async def failing_session() -> AsyncIterator[AsyncSession]:
        async with factory() as own_session:
            yield own_session

    application.dependency_overrides[get_session] = failing_session

    response = await post_payment(lenient_client, PAYMENT_BODY, new_idempotency_key())

    assert response.status_code == 503
    assert await count_rows(session, Payment) == 0
    assert await count_rows(session, OutboxEvent) == 0
