import uuid
from collections.abc import Sequence
from datetime import timedelta

from sqlalchemy import delete, func, literal_column, select, update

from core.config import Settings, get_settings
from core.custom_logging import get_logger
from db.session import AsyncSession
from messaging.names import RoutingKey
from messaging.publisher import EventPublisher
from models import AggregateType, EventType, OutboxEvent, Payment
from schemas.events import PaymentCreatedEvent
from schemas.outbox import OutboxRelayResult
from services.base import BaseDataManager, BaseService

logger = get_logger()


class OutboxRelayService(BaseService[OutboxEvent]):
    def __init__(
        self,
        session: AsyncSession,
        publisher: EventPublisher,
        settings: Settings | None = None,
    ) -> None:
        super().__init__(session)
        self.data_manager = OutboxDataManager(session)
        self.publisher = publisher
        self.settings = settings or get_settings()

    async def relay_batch(self) -> OutboxRelayResult:
        published_ids: list[uuid.UUID] = []
        failed = 0
        async with self.session.transaction():
            events = await self.data_manager.claim_batch(
                self.settings.outbox_batch_size
            )
            for event in events:
                try:
                    await self.publisher.publish(event)
                except Exception as exc:  # noqa: BLE001
                    failed = 1
                    await self._reschedule(event, exc)
                    break
                published_ids.append(event.id)
            await self.data_manager.mark_published(published_ids)

        result = OutboxRelayResult(
            claimed=len(events), published=len(published_ids), failed=failed
        )
        if events:
            logger.info("Outbox batch relayed", **result.model_dump())
        return result

    async def purge_published(self) -> int:
        older_than = timedelta(days=self.settings.outbox_retention_days)
        async with self.session.transaction():
            deleted = await self.data_manager.delete_published_before(older_than)
        if deleted:
            logger.info("Outbox retention purge", deleted=deleted)
        return deleted

    async def backlog(self) -> int:
        async with self.session.transaction():
            return await self.data_manager.count_unpublished()

    async def _reschedule(self, event: OutboxEvent, exc: Exception) -> None:
        error = f"{type(exc).__name__}: {exc}"[
            : self.settings.outbox_last_error_max_length
        ]
        attempts = await self.data_manager.schedule_retry(
            event.id, error, self.settings.outbox_max_backoff_seconds
        )
        alert = attempts >= self.settings.outbox_alert_after_attempts
        fields = {"event_id": str(event.id), "attempts": attempts, "error": error}
        if alert:
            logger.error("Outbox publish failed", **fields)
        else:
            logger.warning("Outbox publish failed", **fields)


class OutboxDataManager(BaseDataManager[OutboxEvent]):
    async def add_payment_created(self, payment: Payment) -> OutboxEvent:
        event_id = uuid.uuid4()
        event = PaymentCreatedEvent(
            event_id=event_id,
            payment_id=payment.id,
            occurred_at=payment.created_at,
        )
        return await self.add_one(
            OutboxEvent(
                id=event_id,
                aggregate_type=AggregateType.PAYMENT,
                aggregate_id=payment.id,
                event_type=EventType.PAYMENT_CREATED,
                routing_key=RoutingKey.PAYMENTS_NEW,
                payload=event.model_dump(mode="json"),
            )
        )

    async def claim_batch(self, limit: int) -> list[OutboxEvent]:
        return await self.get_all(
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.next_attempt_at <= func.now(),
            )
            .order_by(OutboxEvent.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    async def mark_published(self, event_ids: Sequence[uuid.UUID]) -> None:
        if not event_ids:
            return
        await self.session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(event_ids))
            .values(published_at=func.now())
            .execution_options(synchronize_session=False)
        )

    async def schedule_retry(
        self, event_id: uuid.UUID, error: str, max_backoff_seconds: int
    ) -> int:
        max_exponent = max_backoff_seconds.bit_length()
        backoff_seconds = func.least(
            func.power(2, func.least(OutboxEvent.attempts + 1, max_exponent)),
            max_backoff_seconds,
        )
        attempts = await self.session.scalar(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                attempts=OutboxEvent.attempts + 1,
                last_error=error,
                next_attempt_at=func.now()
                + literal_column("interval '1 second'") * backoff_seconds,
            )
            .returning(OutboxEvent.attempts)
            .execution_options(synchronize_session=False)
        )
        return attempts or 0

    async def delete_published_before(self, older_than: timedelta) -> int:
        result = await self.session.execute(
            delete(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_not(None),
                OutboxEvent.published_at < func.now() - older_than,
            )
            .execution_options(synchronize_session=False)
        )
        return result.rowcount

    async def count_unpublished(self) -> int:
        return (
            await self.session.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
            )
            or 0
        )
