import asyncio
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.session import AsyncSession
from models import Payment, PaymentStatus
from schemas.payment import PaymentCreate
from services.exceptions import PaymentNotFoundError
from services.payments import PaymentService
from services.processing import PaymentProcessingService

pytestmark = pytest.mark.database

BODY = {
    "amount": "99.99",
    "currency": "EUR",
    "description": "processing test",
    "webhook_url": "https://merchant.example/hooks",
}


async def create_payment(session: AsyncSession) -> uuid.UUID:
    result = await PaymentService(session).create(
        PaymentCreate.model_validate(BODY), f"proc-{uuid.uuid4()}"
    )
    return result.payment.payment_id


async def stored(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment is not None
    return payment


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_pending_payment_transitions(
    session: AsyncSession, status: PaymentStatus
) -> None:
    payment_id = await create_payment(session)
    before = await stored(session, payment_id)
    created_updated_at = before.updated_at

    transition = await PaymentProcessingService(session).complete(payment_id, status)

    assert transition.changed is True
    assert transition.payment.status is status
    assert transition.payment.processed_at is not None
    payment = await stored(session, payment_id)
    assert payment.status is status
    assert payment.processed_at == transition.payment.processed_at
    assert payment.updated_at > created_updated_at


async def test_first_outcome_wins(session: AsyncSession) -> None:
    payment_id = await create_payment(session)
    service = PaymentProcessingService(session)
    first = await service.complete(payment_id, PaymentStatus.SUCCEEDED)

    second = await service.complete(payment_id, PaymentStatus.FAILED)

    assert second.changed is False
    assert second.payment.status is PaymentStatus.SUCCEEDED
    assert second.payment.processed_at == first.payment.processed_at
    assert (await stored(session, payment_id)).status is PaymentStatus.SUCCEEDED


async def test_duplicate_completion_is_a_no_op(session: AsyncSession) -> None:
    payment_id = await create_payment(session)
    service = PaymentProcessingService(session)
    first = await service.complete(payment_id, PaymentStatus.FAILED)

    duplicate = await service.complete(payment_id, PaymentStatus.FAILED)

    assert duplicate.changed is False
    assert duplicate.payment == first.payment


async def test_concurrent_completions_change_status_once(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    payment_id = await create_payment(session)
    outcomes = [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED] * 5

    async def complete(status: PaymentStatus) -> tuple[bool, PaymentStatus]:
        async with session_factory() as own_session:
            transition = await PaymentProcessingService(own_session).complete(
                payment_id, status
            )
            return transition.changed, transition.payment.status

    results = await asyncio.gather(*(complete(status) for status in outcomes))

    assert sum(changed for changed, _ in results) == 1
    final_status = (await stored(session, payment_id)).status
    assert {status for _, status in results} == {final_status}


async def test_unknown_payment_raises(session: AsyncSession) -> None:
    with pytest.raises(PaymentNotFoundError):
        await PaymentProcessingService(session).complete(
            uuid.uuid4(), PaymentStatus.SUCCEEDED
        )


async def test_pending_is_not_a_valid_outcome(session: AsyncSession) -> None:
    payment_id = await create_payment(session)

    with pytest.raises(ValueError, match="succeeded or failed"):
        await PaymentProcessingService(session).complete(
            payment_id, PaymentStatus.PENDING
        )

    assert (await stored(session, payment_id)).status is PaymentStatus.PENDING


async def test_other_payments_are_untouched(session: AsyncSession) -> None:
    target = await create_payment(session)
    other = await create_payment(session)

    await PaymentProcessingService(session).complete(target, PaymentStatus.SUCCEEDED)

    other_payment = await stored(session, other)
    assert other_payment.status is PaymentStatus.PENDING
    assert other_payment.processed_at is None
    pending = (
        await session.scalars(
            select(Payment.id).where(Payment.status == PaymentStatus.PENDING)
        )
    ).all()
    assert pending == [other]
