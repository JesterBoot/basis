import asyncio
import uuid
from datetime import datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.session import AsyncSession
from models import OutboxEvent, Payment, PaymentStatus
from tests.support.api import (
    PAYMENT_BODY,
    PAYMENTS_URL,
    nested_metadata,
    new_idempotency_key,
    post_payment,
)
from tests.support.database import count_rows

pytestmark = pytest.mark.database


async def test_create_payment_returns_202(
    client: AsyncClient, session: AsyncSession
) -> None:
    response = await post_payment(client, PAYMENT_BODY, new_idempotency_key())

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert uuid.UUID(data["payment_id"])
    assert datetime.fromisoformat(data["created_at"]).tzinfo is not None
    assert "Idempotent-Replayed" not in response.headers


async def test_created_response_is_json_with_utc_timestamp(
    client: AsyncClient, session: AsyncSession
) -> None:
    response = await post_payment(client, PAYMENT_BODY, new_idempotency_key())

    assert response.headers["Content-Type"] == "application/json"
    assert response.json()["created_at"].endswith("Z")


async def test_replay_returns_same_response_with_header(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    first = await post_payment(client, PAYMENT_BODY, idempotency_key)

    second = await post_payment(
        client, {**PAYMENT_BODY, "amount": 1500}, idempotency_key
    )

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count_rows(session, Payment) == 1
    assert await count_rows(session, OutboxEvent) == 1


async def test_e_a7_reordered_padded_and_case_variant_body_replays(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    first = await post_payment(client, PAYMENT_BODY, idempotency_key)
    variant = {
        "webhook_url": "https://MERCHANT.example/hooks/payments",
        "metadata": {"order_id": 42},
        "description": "  Order #42  ",
        "currency": "RUB",
        "amount": 1500.0,
    }

    second = await post_payment(client, variant, idempotency_key)

    assert second.status_code == 202
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()


async def test_e_a7_deep_unicode_metadata_round_trips_and_replays(
    client: AsyncClient, session: AsyncSession
) -> None:
    metadata = {"ключ": "значение 🍕", "tree": nested_metadata(20), "big": 10**40}
    body = {**PAYMENT_BODY, "description": "Заказ №42 🍕", "metadata": metadata}
    idempotency_key = new_idempotency_key()
    created = await post_payment(client, body, idempotency_key)

    replay = await post_payment(client, body, idempotency_key)
    fetched = await client.get(f"{PAYMENTS_URL}/{created.json()['payment_id']}")

    assert created.status_code == 202
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert fetched.json()["metadata"] == metadata
    assert fetched.json()["description"] == "Заказ №42 🍕"


async def test_e_a4_idempotency_key_is_case_sensitive(
    client: AsyncClient, session: AsyncSession
) -> None:
    first = await post_payment(client, PAYMENT_BODY, "Order-Key")

    second = await post_payment(client, PAYMENT_BODY, "order-key")

    assert second.status_code == 202
    assert "Idempotent-Replayed" not in second.headers
    assert second.json()["payment_id"] != first.json()["payment_id"]
    assert await count_rows(session, Payment) == 2


async def test_reused_key_with_different_body_returns_409(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    await post_payment(client, PAYMENT_BODY, idempotency_key)

    response = await post_payment(
        client, {**PAYMENT_BODY, "currency": "USD"}, idempotency_key
    )

    assert response.status_code == 409
    assert response.json() == {
        "detail": "Idempotency-Key already used with a different request body"
    }
    assert await count_rows(session, Payment) == 1


async def test_e_a6_conflict_writes_nothing_and_keeps_original(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    await post_payment(client, PAYMENT_BODY, idempotency_key)

    conflict = await post_payment(
        client, {**PAYMENT_BODY, "amount": "1500.01"}, idempotency_key
    )

    assert conflict.status_code == 409
    assert "Idempotent-Replayed" not in conflict.headers
    assert await count_rows(session, Payment) == 1
    assert await count_rows(session, OutboxEvent) == 1
    stored = await session.scalar(select(Payment))
    assert stored is not None
    assert str(stored.amount) == "1500.00"


async def test_e_a6_metadata_value_type_change_is_a_conflict(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    await post_payment(client, PAYMENT_BODY, idempotency_key)

    conflict = await post_payment(
        client, {**PAYMENT_BODY, "metadata": {"order_id": "42"}}, idempotency_key
    )

    assert conflict.status_code == 409


async def test_e_a8_replay_after_success_returns_current_status(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    created = (await post_payment(client, PAYMENT_BODY, idempotency_key)).json()
    await session.execute(
        update(Payment)
        .where(Payment.idempotency_key == idempotency_key)
        .values(status=PaymentStatus.SUCCEEDED)
    )
    await session.commit()

    replay = await post_payment(client, PAYMENT_BODY, idempotency_key)

    assert replay.status_code == 202
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json() == {**created, "status": "succeeded"}
    assert await count_rows(session, OutboxEvent) == 1


async def test_concurrent_posts_with_same_key_share_one_payment(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()

    responses = await asyncio.gather(
        *(post_payment(client, PAYMENT_BODY, idempotency_key) for _ in range(20))
    )

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert sum("Idempotent-Replayed" not in r.headers for r in responses) == 1
    assert await count_rows(session, Payment) == 1
    assert await count_rows(session, OutboxEvent) == 1


async def test_e_a5_concurrent_mixed_bodies_yield_only_202_or_409(
    client: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = new_idempotency_key()
    bodies = [PAYMENT_BODY, {**PAYMENT_BODY, "currency": "USD"}] * 10

    responses = await asyncio.gather(
        *(post_payment(client, body, idempotency_key) for body in bodies)
    )

    assert {r.status_code for r in responses} == {202, 409}
    accepted = [r for r in responses if r.status_code == 202]
    assert len({r.json()["payment_id"] for r in accepted}) == 1
    assert await count_rows(session, Payment) == 1
    assert await count_rows(session, OutboxEvent) == 1


async def test_e_a5_concurrent_distinct_keys_create_distinct_payments(
    client: AsyncClient, session: AsyncSession
) -> None:
    responses = await asyncio.gather(
        *(post_payment(client, PAYMENT_BODY, new_idempotency_key()) for _ in range(20))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 20
    assert await count_rows(session, OutboxEvent) == 20


async def test_e_a5_waiter_inserts_when_first_transaction_rolls_back(
    client: AsyncClient,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    idempotency_key = new_idempotency_key()

    async with session_factory() as holder:
        await holder.execute(
            insert(Payment).values(
                id=uuid.uuid4(),
                amount=PAYMENT_BODY["amount"],
                currency=PAYMENT_BODY["currency"],
                description="uncommitted competitor",
                meta={},
                idempotency_key=idempotency_key,
                request_hash="0" * 64,
                webhook_url=PAYMENT_BODY["webhook_url"],
            )
        )
        task = asyncio.create_task(post_payment(client, PAYMENT_BODY, idempotency_key))
        await asyncio.sleep(0.2)
        blocked = not task.done()
        await holder.rollback()

    response = await task
    assert blocked
    assert response.status_code == 202
    assert "Idempotent-Replayed" not in response.headers
    assert await count_rows(session, Payment) == 1
    assert await count_rows(session, OutboxEvent) == 1
