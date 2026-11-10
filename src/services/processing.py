import uuid

from core.custom_logging import get_logger
from db.session import AsyncSession
from models import Payment, PaymentStatus
from schemas.payment import PaymentRead, PaymentTransition
from services.base import BaseService
from services.exceptions import PaymentNotFoundError
from services.payments import PaymentDataManager

logger = get_logger()


class PaymentProcessingService(BaseService[Payment]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.data_manager = PaymentDataManager(session)

    async def complete(
        self, payment_id: uuid.UUID, status: PaymentStatus
    ) -> PaymentTransition:
        if status is PaymentStatus.PENDING:
            raise ValueError("A payment can only be completed as succeeded or failed")

        async with self.session.transaction():
            payment = await self.data_manager.mark_processed(payment_id, status)
            changed = payment is not None
            if payment is None:
                payment = await self.data_manager.get_by_pk(payment_id)

        if payment is None:
            raise PaymentNotFoundError(payment_id)

        fields = {"payment_id": str(payment_id), "status": payment.status.value}
        if changed:
            logger.info("Payment processed", **fields)
        else:
            logger.info("Payment already processed, skipping transition", **fields)
        return PaymentTransition(
            payment=PaymentRead.model_validate(payment), changed=changed
        )
