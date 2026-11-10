import uuid
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.session import AsyncSession
from models import AggregateType, Currency, EventType, OutboxEvent, Payment
from services.base import BaseDataManager
from services.outbox import OutboxDataManager

pytestmark = pytest.mark.database


class PaymentManager(BaseDataManager[Payment]):
    pass


def make_payment(**overrides: Any) -> Payment:
    values: dict[str, Any] = {
        "amount": Decimal("10.00"),
        "currency": Currency.USD,
        "description": "data manager test",
        "idempotency_key": f"dm-{uuid.uuid4()}",
        "request_hash": "b" * 64,
        "webhook_url": "https://merchant.example/hooks",
    }
    values.update(overrides)
    return Payment(**values)


async def committed_count(
    factory: async_sessionmaker[AsyncSession], model: type[Payment] | type[OutboxEvent]
) -> int:
    async with factory() as other_session:
        return await other_session.scalar(select(func.count()).select_from(model)) or 0


async def test_add_one_flushes_and_loads_server_defaults(session: AsyncSession) -> None:
    payment = await PaymentManager(session).add_one(make_payment())

    assert payment.created_at is not None
    assert payment.updated_at is not None
    assert payment.meta == {}


async def test_add_one_does_not_commit(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await PaymentManager(session).add_one(make_payment())

    assert await committed_count(session_factory, Payment) == 0
    await session.rollback()
    assert await session.scalar(select(func.count()).select_from(Payment)) == 0


async def test_add_all_flushes_without_committing(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await PaymentManager(session).add_all([make_payment(), make_payment()])

    assert await session.scalar(select(func.count()).select_from(Payment)) == 2
    assert await committed_count(session_factory, Payment) == 0


async def test_get_one_and_get_all(session: AsyncSession) -> None:
    manager = PaymentManager(session)
    first = await manager.add_one(make_payment(description="first"))
    await manager.add_one(make_payment(description="second"))

    found = await manager.get_one(select(Payment).where(Payment.id == first.id))
    missing = await manager.get_one(select(Payment).where(Payment.id == uuid.uuid4()))
    everything = await manager.get_all(select(Payment).order_by(Payment.description))

    assert found is first
    assert missing is None
    assert [payment.description for payment in everything] == ["first", "second"]


async def test_update_flushes_and_refreshes_without_committing(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    manager = PaymentManager(session)
    payment = await manager.add_one(make_payment())
    await session.commit()

    payment.description = "changed"
    updated = await manager.update(payment)

    assert updated.description == "changed"
    async with session_factory() as other_session:
        stored = await other_session.get(Payment, payment.id)
        assert stored is not None
        assert stored.description == "data manager test"


async def test_outbox_payment_created_event(session: AsyncSession) -> None:
    payment = await PaymentManager(session).add_one(make_payment())

    event = await OutboxDataManager(session).add_payment_created(payment)

    assert event.aggregate_type == AggregateType.PAYMENT == "payment"
    assert event.aggregate_id == payment.id
    assert event.event_type == EventType.PAYMENT_CREATED == "payment.created"
    assert event.routing_key == "payments.new"
    assert event.published_at is None
    assert event.attempts == 0
    assert event.payload == {
        "event_id": str(event.id),
        "event_type": "payment.created",
        "payment_id": str(payment.id),
        "occurred_at": payment.created_at.isoformat().replace("+00:00", "Z"),
    }


async def test_outbox_event_is_not_committed_by_data_manager(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment = await PaymentManager(session).add_one(make_payment())

    await OutboxDataManager(session).add_payment_created(payment)

    assert await committed_count(session_factory, OutboxEvent) == 0


async def test_outbox_stores_enum_values_as_plain_strings(
    session: AsyncSession,
) -> None:
    payment = await PaymentManager(session).add_one(make_payment())
    event = await OutboxDataManager(session).add_payment_created(payment)
    await session.commit()

    raw = (
        await session.execute(
            select(
                OutboxEvent.__table__.c.aggregate_type,
                OutboxEvent.__table__.c.event_type,
                OutboxEvent.__table__.c.routing_key,
            ).where(OutboxEvent.__table__.c.id == event.id)
        )
    ).one()

    assert tuple(raw) == ("payment", "payment.created", "payments.new")
