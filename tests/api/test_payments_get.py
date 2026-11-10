import uuid
from collections.abc import Callable

import pytest
from httpx import AsyncClient

from db.session import AsyncSession
from tests.support.api import (
    PAYMENT_BODY,
    PAYMENTS_URL,
    new_idempotency_key,
    post_payment,
)


async def test_get_with_malformed_id_is_rejected(client: AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/not-a-uuid")

    assert response.status_code == 422


@pytest.mark.parametrize(
    "payment_id",
    [
        str(uuid.uuid4())[:-1],
        f"{uuid.uuid4()}0",
        "g" * 32,
        "123",
        "00000000-0000-0000-0000-00000000000",
    ],
    ids=["truncated", "extra-char", "non-hex", "integer", "short-group"],
)
async def test_e_a13_near_miss_uuids_are_rejected(
    client: AsyncClient, payment_id: str
) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{payment_id}")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "payment_id"]


@pytest.mark.database
async def test_get_payment_returns_details(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    created = (await post_payment(client, PAYMENT_BODY, idempotency_key)).json()

    response = await client.get(f"{PAYMENTS_URL}/{created['payment_id']}")

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
@pytest.mark.parametrize(
    "transform",
    [str.upper, lambda value: value.replace("-", "")],
    ids=["uppercase", "no-hyphens"],
)
async def test_e_a13_alternate_uuid_spellings_resolve(
    client: AsyncClient, session: AsyncSession, transform: Callable[[str], str]
) -> None:
    payment_id = (
        await post_payment(client, PAYMENT_BODY, new_idempotency_key())
    ).json()["payment_id"]

    response = await client.get(f"{PAYMENTS_URL}/{transform(payment_id)}")

    assert response.status_code == 200
    assert response.json()["payment_id"] == payment_id


@pytest.mark.database
async def test_get_unknown_payment_returns_404(
    client: AsyncClient, session: AsyncSession
) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{uuid.uuid4()}")

    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}


@pytest.mark.database
async def test_e_a13_unknown_id_returns_json_404(
    client: AsyncClient, session: AsyncSession
) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{uuid.UUID(int=0)}")

    assert response.status_code == 404
    assert response.headers["Content-Type"] == "application/json"
