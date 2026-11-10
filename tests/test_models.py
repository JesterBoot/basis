import uuid
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError, StatementError

from db.session import AsyncSession
from models import Currency, OutboxEvent, Payment, PaymentStatus, WebhookStatus

pytestmark = pytest.mark.database

RAW_PAYMENT_INSERT = text(
    """
    INSERT INTO payments (
        id, amount, currency, description, status, idempotency_key,
        request_hash, webhook_url, webhook_status, webhook_attempts
    ) VALUES (
        gen_random_uuid(), :amount, :currency, 'raw', :status, :key,
        repeat('a', 64), 'https://example.com/hook', :webhook_status, :webhook_attempts
    )
    """
)
VALID_RAW_VALUES: dict[str, Any] = {
    "amount": Decimal("1.00"),
    "currency": "USD",
    "status": "pending",
    "webhook_status": "pending",
    "webhook_attempts": 0,
}


def make_payment(**overrides: Any) -> Payment:
    values: dict[str, Any] = {
        "amount": Decimal("100.00"),
        "currency": Currency.USD,
        "description": "Order #1",
        "idempotency_key": f"key-{uuid.uuid4()}",
        "request_hash": "a" * 64,
        "webhook_url": "https://merchant.example/hooks",
    }
    values.update(overrides)
    return Payment(**values)


async def test_payment_defaults_are_applied(session: AsyncSession) -> None:
    payment = make_payment()
    session.add(payment)
    await session.commit()
    await session.refresh(payment)

    assert isinstance(payment.id, uuid.UUID)
    assert payment.status is PaymentStatus.PENDING
    assert payment.webhook_status is WebhookStatus.PENDING
    assert payment.webhook_attempts == 0
    assert payment.meta == {}
    assert payment.created_at.tzinfo is not None
    assert payment.updated_at.tzinfo is not None
    assert payment.processed_at is None
    assert payment.webhook_delivered_at is None


async def test_tables_are_truncated_between_tests(session: AsyncSession) -> None:
    assert await session.scalar(select(func.count()).select_from(Payment)) == 0
    assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


async def test_meta_attribute_is_stored_in_metadata_column(
    session: AsyncSession,
) -> None:
    payment = make_payment(
        meta={"order_id": 42, "tags": ["a", "b"], "nested": {"x": 1}}
    )
    session.add(payment)
    await session.commit()

    stored = await session.scalar(
        text("SELECT metadata FROM payments WHERE id = :id"), {"id": payment.id}
    )

    assert stored == {"order_id": 42, "tags": ["a", "b"], "nested": {"x": 1}}


async def test_amount_is_stored_with_two_decimal_places(session: AsyncSession) -> None:
    payment = make_payment(amount=Decimal("10.5"))
    session.add(payment)
    await session.commit()
    await session.refresh(payment)

    assert payment.amount == Decimal("10.50")
    assert str(payment.amount) == "10.50"


async def test_duplicate_idempotency_key_is_rejected(session: AsyncSession) -> None:
    session.add(make_payment(idempotency_key="same-key"))
    await session.commit()

    session.add(make_payment(idempotency_key="same-key"))
    with pytest.raises(IntegrityError, match="uq_payments_idempotency_key"):
        await session.commit()


@pytest.mark.parametrize(
    ("column", "value", "constraint"),
    [
        ("amount", Decimal(0), "ck_payments_amount_positive"),
        ("amount", Decimal("-5.00"), "ck_payments_amount_positive"),
        ("currency", "GBP", "ck_payments_currency"),
        ("currency", "usd", "ck_payments_currency"),
        ("status", "done", "ck_payments_status"),
        ("webhook_status", "sent", "ck_payments_webhook_status"),
        ("webhook_attempts", -1, "ck_payments_webhook_attempts_non_negative"),
    ],
)
async def test_database_check_constraints(
    session: AsyncSession, column: str, value: Any, constraint: str
) -> None:
    values = {**VALID_RAW_VALUES, column: value, "key": f"raw-{uuid.uuid4()}"}

    with pytest.raises(IntegrityError, match=constraint):
        await session.execute(RAW_PAYMENT_INSERT, values)


async def test_orm_rejects_unknown_currency_before_hitting_database(
    session: AsyncSession,
) -> None:
    session.add(make_payment(currency="GBP"))

    with pytest.raises(StatementError):
        await session.commit()


async def test_updated_at_changes_on_update(session: AsyncSession) -> None:
    payment = make_payment()
    session.add(payment)
    await session.commit()
    await session.refresh(payment)
    created_updated_at = payment.updated_at

    payment.status = PaymentStatus.SUCCEEDED
    await session.commit()
    await session.refresh(payment)

    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.updated_at > created_updated_at


async def test_outbox_event_defaults_are_applied(session: AsyncSession) -> None:
    payment_id = uuid.uuid4()
    event = OutboxEvent(
        aggregate_type="payment",
        aggregate_id=payment_id,
        event_type="payment.created",
        routing_key="payments.new",
        payload={"payment_id": str(payment_id)},
    )
    session.add(event)
    await session.commit()
    await session.refresh(event)

    assert isinstance(event.id, uuid.UUID)
    assert event.attempts == 0
    assert event.published_at is None
    assert event.last_error is None
    assert event.next_attempt_at.tzinfo is not None
    assert event.payload == {"payment_id": str(payment_id)}
