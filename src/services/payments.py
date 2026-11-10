import uuid
from typing import Annotated

from fastapi import Depends
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from db.session import AsyncSession, get_session
from models import Payment, PaymentStatus, WebhookStatus
from schemas.payment import (
    PaymentCreate,
    PaymentCreated,
    PaymentCreateResult,
    PaymentRead,
)
from services.base import BaseDataManager, BaseService
from services.exceptions import IdempotencyKeyReusedError, PaymentNotFoundError
from services.outbox import OutboxDataManager


class PaymentService(BaseService[Payment]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.data_manager = PaymentDataManager(session)
        self.outbox = OutboxDataManager(session)

    async def create(
        self, payload: PaymentCreate, idempotency_key: str
    ) -> PaymentCreateResult:
        request_hash = payload.request_hash()
        async with self.session.transaction():
            payment = await self.data_manager.insert_if_absent(
                payload, idempotency_key, request_hash
            )
            if payment is not None:
                await self.outbox.add_payment_created(payment)
                return PaymentCreateResult(
                    payment=PaymentCreated.model_validate(payment), replayed=False
                )
            existing = await self.data_manager.get_by_idempotency_key(idempotency_key)

        if existing is None:
            raise RuntimeError("Idempotency key conflict without an existing payment")
        if existing.request_hash != request_hash:
            raise IdempotencyKeyReusedError(idempotency_key)
        return PaymentCreateResult(
            payment=PaymentCreated.model_validate(existing), replayed=True
        )

    async def get(self, payment_id: uuid.UUID) -> PaymentRead:
        payment = await self.data_manager.get_by_pk(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return PaymentRead.model_validate(payment)


class PaymentDataManager(BaseDataManager[Payment]):
    async def get_by_pk(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return await self.get_one(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )

    async def mark_processed(
        self, payment_id: uuid.UUID, status: PaymentStatus
    ) -> Payment | None:
        return await self.get_one(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
            .returning(Payment)
            .execution_options(populate_existing=True)
        )

    async def record_webhook_attempt(
        self,
        payment_id: uuid.UUID,
        *,
        status: WebhookStatus | None,
        error: str | None,
    ) -> int:
        values: dict[str, object] = {
            "webhook_attempts": Payment.webhook_attempts + 1,
            "webhook_last_error": error,
        }
        if status is not None:
            values["webhook_status"] = status
        if status is WebhookStatus.DELIVERED:
            values["webhook_delivered_at"] = func.now()
        attempts = await self.session.scalar(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(**values)
            .returning(Payment.webhook_attempts)
            .execution_options(synchronize_session=False)
        )
        return attempts or 0

    async def mark_webhook_failed(self, payment_id: uuid.UUID) -> None:
        await self.session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.webhook_status != WebhookStatus.DELIVERED,
            )
            .values(webhook_status=WebhookStatus.FAILED)
            .execution_options(synchronize_session=False)
        )

    async def insert_if_absent(
        self, payload: PaymentCreate, idempotency_key: str, request_hash: str
    ) -> Payment | None:
        statement = (
            insert(Payment)
            .values(
                id=uuid.uuid4(),
                amount=payload.amount,
                currency=payload.currency,
                description=payload.description,
                meta=payload.metadata,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                webhook_url=str(payload.webhook_url),
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return await self.get_one(statement)


async def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentService:
    return PaymentService(session)
