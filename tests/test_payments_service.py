import asyncio
import uuid
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.session import AsyncSession
from models import OutboxEvent, Payment, PaymentStatus
from schemas.payment import PaymentCreate
from services.exceptions import IdempotencyKeyReusedError, PaymentNotFoundError
from services.outbox import OutboxDataManager
from services.payments import PaymentDataManager, PaymentService

pytestmark = pytest.mark.database

BODY: dict[str, Any] = {
    "amount": "250.50",
    "currency": "EUR",
    "description": "Order #7",
    "metadata": {"order_id": 7},
    "webhook_url": "https://merchant.example/hooks",
}


def payload(**overrides: Any) -> PaymentCreate:
    return PaymentCreate.model_validate({**BODY, **overrides})


async def count(session: AsyncSession, model: type[Payment] | type[OutboxEvent]) -> int:
    return await session.scalar(select(func.count()).select_from(model)) or 0


async def test_new_payment_is_stored_with_its_outbox_event(
    session: AsyncSession,
) -> None:
    request = payload()

    result = await PaymentService(session).create(request, "key-new")

    assert result.replayed is False
    assert result.payment.status is PaymentStatus.PENDING
    payment = await session.get(Payment, result.payment.payment_id)
    assert payment is not None
    assert payment.created_at == result.payment.created_at
    assert payment.idempotency_key == "key-new"
    assert payment.request_hash == request.request_hash()
    assert payment.meta == {"order_id": 7}
    assert payment.webhook_url == "https://merchant.example/hooks"

    events = (await session.scalars(select(OutboxEvent))).all()
    assert len(events) == 1
    event = events[0]
    assert event.aggregate_type == "payment"
    assert event.aggregate_id == payment.id
    assert event.event_type == "payment.created"
    assert event.routing_key == "payments.new"
    assert event.published_at is None
    assert event.payload == {
        "event_id": str(event.id),
        "event_type": "payment.created",
        "payment_id": str(payment.id),
        "occurred_at": payment.created_at.isoformat().replace("+00:00", "Z"),
    }


async def test_same_key_and_body_replays_original_payment(
    session: AsyncSession,
) -> None:
    service = PaymentService(session)
    first = await service.create(payload(), "key-replay")

    second = await service.create(payload(amount=250.5), "key-replay")

    assert second.replayed is True
    assert second.payment == first.payment
    assert await count(session, Payment) == 1
    assert await count(session, OutboxEvent) == 1


async def test_same_key_with_different_body_is_rejected(session: AsyncSession) -> None:
    service = PaymentService(session)
    await service.create(payload(), "key-conflict")

    with pytest.raises(IdempotencyKeyReusedError):
        await service.create(payload(amount="999.00"), "key-conflict")

    assert await count(session, Payment) == 1
    assert await count(session, OutboxEvent) == 1


async def test_different_keys_create_separate_payments(session: AsyncSession) -> None:
    service = PaymentService(session)
    first = await service.create(payload(), "key-a")
    second = await service.create(payload(), "key-b")

    assert first.payment.payment_id != second.payment.payment_id
    assert await count(session, Payment) == 2
    assert await count(session, OutboxEvent) == 2


async def test_replay_returns_current_status(session: AsyncSession) -> None:
    service = PaymentService(session)
    first = await service.create(payload(), "key-status")
    await session.execute(
        update(Payment)
        .where(Payment.id == first.payment.payment_id)
        .values(status=PaymentStatus.SUCCEEDED)
    )
    await session.commit()

    replay = await service.create(payload(), "key-status")

    assert replay.replayed is True
    assert replay.payment.status is PaymentStatus.SUCCEEDED


async def test_payment_is_rolled_back_when_outbox_write_fails(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def broken_add(manager: OutboxDataManager, payment: Payment) -> OutboxEvent:
        return await manager.add_one(
            OutboxEvent(
                aggregate_type=None,
                aggregate_id=payment.id,
                event_type="payment.created",
                routing_key="payments.new",
                payload={},
            )
        )

    monkeypatch.setattr(OutboxDataManager, "add_payment_created", broken_add)

    async with session_factory() as failing_session:
        with pytest.raises(IntegrityError, match="aggregate_type"):
            await PaymentService(failing_session).create(payload(), "key-atomic")

    assert await count(session, Payment) == 0
    assert await count(session, OutboxEvent) == 0


async def test_concurrent_requests_with_same_key_create_one_payment(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    request = payload()

    async def submit() -> tuple[uuid.UUID, bool]:
        async with session_factory() as own_session:
            result = await PaymentService(own_session).create(request, "key-race")
            return result.payment.payment_id, result.replayed

    results = await asyncio.gather(*(submit() for _ in range(20)))

    assert len({payment_id for payment_id, _ in results}) == 1
    assert sum(1 for _, replayed in results if not replayed) == 1
    assert await count(session, Payment) == 1
    assert await count(session, OutboxEvent) == 1


async def test_concurrent_conflicting_bodies_keep_only_the_winner(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async def submit(amount: str) -> str:
        async with session_factory() as own_session:
            try:
                await PaymentService(own_session).create(
                    payload(amount=amount), "key-mixed"
                )
            except IdempotencyKeyReusedError:
                return "conflict"
            return amount

    outcomes = await asyncio.gather(*(submit(f"{n}.00") for n in range(1, 11)))

    winners = [outcome for outcome in outcomes if outcome != "conflict"]
    assert len(winners) == 1
    stored = await session.scalar(select(Payment))
    assert stored is not None
    assert str(stored.amount) == winners[0]
    assert await count(session, OutboxEvent) == 1


async def test_get_returns_payment_details(session: AsyncSession) -> None:
    service = PaymentService(session)
    created = await service.create(payload(), "key-get")

    details = await service.get(created.payment.payment_id)

    assert details.payment_id == created.payment.payment_id
    assert str(details.amount) == "250.50"
    assert details.metadata == {"order_id": 7}
    assert details.idempotency_key == "key-get"


async def test_get_unknown_payment_raises(session: AsyncSession) -> None:
    with pytest.raises(PaymentNotFoundError):
        await PaymentService(session).get(uuid.uuid4())


async def test_conflict_without_existing_row_is_an_internal_error(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def nothing(*_: object) -> None:
        return None

    monkeypatch.setattr(PaymentDataManager, "insert_if_absent", nothing)
    monkeypatch.setattr(PaymentDataManager, "get_by_idempotency_key", nothing)

    with pytest.raises(RuntimeError, match="without an existing payment"):
        await PaymentService(session).create(payload(), "key-ghost")
