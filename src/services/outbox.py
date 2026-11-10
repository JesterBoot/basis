import uuid

from messaging.names import RoutingKey
from models import AggregateType, EventType, OutboxEvent, Payment
from schemas.events import PaymentCreatedEvent
from services.base import BaseDataManager


class OutboxDataManager(BaseDataManager[OutboxEvent]):
    # M4: claim_batch / mark_published / schedule_retry for the outbox relay

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
