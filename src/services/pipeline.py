import uuid

from pydantic import ValidationError

from core.config import Settings, get_settings
from core.custom_logging import get_logger
from db.session import tasks_session_context
from models import PaymentStatus, WebhookStatus
from schemas.events import PaymentCreatedEvent
from schemas.payment import PaymentRead
from schemas.pipeline import PipelineOutcome, PipelineResult
from services.exceptions import (
    PaymentNotFoundError,
    WebhookPermanentError,
    WebhookTransientError,
)
from services.gateway import PaymentGateway
from services.payments import PaymentService
from services.processing import PaymentProcessingService
from services.webhooks import WebhookClient, WebhookService

logger = get_logger()


class PaymentPipeline:
    def __init__(
        self,
        gateway: PaymentGateway,
        webhook_client: WebhookClient,
        settings: Settings | None = None,
    ) -> None:
        self.gateway = gateway
        self.webhook_client = webhook_client
        self.settings = settings or get_settings()

    async def process(self, body: bytes) -> PipelineResult:
        try:
            event = PaymentCreatedEvent.model_validate_json(body)
        except ValidationError:
            return self._dead_letter(
                "Message body is not a valid payment.created event"
            )

        payment = await self._load(event)
        if payment is None:
            return self._dead_letter(f"Payment {event.payment_id} does not exist")

        if payment.status is PaymentStatus.PENDING:
            payment = await self._charge(payment)

        if payment.webhook_status is not WebhookStatus.PENDING:
            return PipelineResult(outcome=PipelineOutcome.DONE)

        return await self._notify(payment, event)

    async def _load(self, event: PaymentCreatedEvent) -> PaymentRead | None:
        async with tasks_session_context() as session:
            try:
                return await PaymentService(session).get(event.payment_id)
            except PaymentNotFoundError:
                return None

    async def _charge(self, payment: PaymentRead) -> PaymentRead:
        outcome = await self.gateway.charge(payment.payment_id)
        async with tasks_session_context() as session:
            transition = await PaymentProcessingService(session).complete(
                payment.payment_id, outcome
            )
        return transition.payment

    async def _notify(
        self, payment: PaymentRead, event: PaymentCreatedEvent
    ) -> PipelineResult:
        async with tasks_session_context() as session:
            service = WebhookService(session, self.webhook_client, self.settings)
            try:
                await service.deliver(payment, event.event_id)
            except WebhookPermanentError as exc:
                return PipelineResult(outcome=PipelineOutcome.DONE, reason=str(exc))
            except WebhookTransientError as exc:
                return PipelineResult(
                    outcome=PipelineOutcome.RETRY,
                    reason=str(exc),
                    payment_id=payment.payment_id,
                )
        return PipelineResult(outcome=PipelineOutcome.DONE)

    async def give_up(self, payment_id: uuid.UUID) -> None:
        async with tasks_session_context() as session:
            service = WebhookService(session, self.webhook_client, self.settings)
            await service.mark_failed(payment_id)

    @staticmethod
    def _dead_letter(reason: str) -> PipelineResult:
        logger.error("Unprocessable payment message", reason=reason)
        return PipelineResult(outcome=PipelineOutcome.DEAD_LETTER, reason=reason)
