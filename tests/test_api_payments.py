import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.config import Settings
from db.session import AsyncSession, build_async_engine, get_session
from main import create_app
from models import OutboxEvent, Payment
from tests.support.api import TEST_API_KEY

BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/hooks/payments",
}


def key() -> str:
    return f"test-{uuid.uuid4()}"


async def post_payment(
    client: AsyncClient, body: dict[str, Any], idempotency_key: str
) -> Any:
    return await client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": idempotency_key}
    )


@pytest.mark.parametrize(
    "idempotency_key",
    ["", "a" * 256, "has space", "slash/key", "кириллица"],
    ids=["empty", "too-long", "space", "slash", "non-ascii"],
)
async def test_invalid_idempotency_key_is_rejected(
    client: AsyncClient, idempotency_key: str
) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=BODY,
        headers={"Idempotency-Key": idempotency_key.encode()},
    )

    assert response.status_code == 422


async def test_missing_idempotency_key_is_rejected(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


async def test_invalid_body_is_rejected(client: AsyncClient) -> None:
    response = await post_payment(client, {**BODY, "amount": "10.999"}, key())

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "amount"]


async def test_get_with_malformed_id_is_rejected(client: AsyncClient) -> None:
    response = await client.get("/api/v1/payments/not-a-uuid")

    assert response.status_code == 422


@pytest.mark.database
async def test_create_payment_returns_202(
    client: AsyncClient, session: AsyncSession
) -> None:
    response = await post_payment(client, BODY, key())

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert uuid.UUID(data["payment_id"])
    assert datetime.fromisoformat(data["created_at"]).tzinfo is not None
    assert "Idempotent-Replayed" not in response.headers


@pytest.mark.database
async def test_replay_returns_same_response_with_header(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    first = await post_payment(client, BODY, idempotency_key)

    second = await post_payment(client, {**BODY, "amount": 1500}, idempotency_key)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await session.scalar(select(func.count()).select_from(Payment)) == 1
    assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.database
async def test_reused_key_with_different_body_returns_409(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    await post_payment(client, BODY, idempotency_key)

    response = await post_payment(client, {**BODY, "currency": "USD"}, idempotency_key)

    assert response.status_code == 409
    assert response.json() == {
        "detail": "Idempotency-Key already used with a different request body"
    }
    assert await session.scalar(select(func.count()).select_from(Payment)) == 1


@pytest.mark.database
async def test_concurrent_posts_with_same_key_share_one_payment(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()

    responses = await asyncio.gather(
        *(post_payment(client, BODY, idempotency_key) for _ in range(20))
    )

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert sum("Idempotent-Replayed" not in r.headers for r in responses) == 1
    assert await session.scalar(select(func.count()).select_from(Payment)) == 1
    assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.database
async def test_get_payment_returns_details(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    created = (await post_payment(client, BODY, idempotency_key)).json()

    response = await client.get(f"/api/v1/payments/{created['payment_id']}")

    assert response.status_code == 200
    data = response.json()
    assert data == {
        "payment_id": created["payment_id"],
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "status": "pending",
        "idempotency_key": idempotency_key,
        "webhook_url": "https://merchant.example/hooks/payments",
        "webhook_status": "pending",
        "webhook_attempts": 0,
        "created_at": created["created_at"],
        "updated_at": data["updated_at"],
        "processed_at": None,
    }
    assert "request_hash" not in data
    assert "webhook_last_error" not in data


@pytest.mark.database
async def test_get_unknown_payment_returns_404(
    client: AsyncClient, session: AsyncSession
) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")

    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}


@pytest.fixture
async def client_with_unreachable_database() -> AsyncIterator[AsyncClient]:
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

    application = create_app()
    application.dependency_overrides[get_session] = unreachable_session
    async with AsyncClient(
        transport=ASGITransport(app=application),
        base_url="http://testserver",
        headers={"X-API-Key": TEST_API_KEY},
    ) as http_client:
        yield http_client
    await engine.dispose()


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid.uuid4()}")],
    ids=["create", "get"],
)
async def test_database_outage_returns_503(
    client_with_unreachable_database: AsyncClient, method: str, path: str
) -> None:
    response = await client_with_unreachable_database.request(
        method, path, json=BODY, headers={"Idempotency-Key": key()}
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert response.headers["Retry-After"] == "5"
