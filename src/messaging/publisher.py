from typing import Protocol

from faststream.rabbit import RabbitBroker

from messaging.names import Exchange, Header
from models import OutboxEvent


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None: ...


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, timeout: float) -> None:
        self.broker = broker
        self.timeout = timeout

    async def publish(self, event: OutboxEvent) -> None:
        await self.broker.publish(
            event.payload,
            exchange=Exchange.PAYMENTS,
            routing_key=event.routing_key,
            mandatory=True,
            persist=True,
            timeout=self.timeout,
            message_id=str(event.id),
            message_type=event.event_type,
            content_type="application/json",
            headers={Header.ATTEMPT: 1},
        )
