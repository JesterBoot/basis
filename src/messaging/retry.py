from typing import Any

from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from core.config import Settings
from messaging.names import Header
from messaging.topology import PaymentsTopology


class RetryScheduler:
    def __init__(
        self, broker: RabbitBroker, settings: Settings, timeout: float | None = None
    ) -> None:
        self.broker = broker
        self.topology = PaymentsTopology(settings)
        self.max_attempts = settings.retry_max_attempts
        self.timeout = timeout or settings.outbox_publish_timeout

    @staticmethod
    def attempt_of(headers: dict[str, Any] | None) -> int:
        raw = (headers or {}).get(Header.ATTEMPT, 1)
        try:
            attempt = int(raw)
        except (TypeError, ValueError):
            return 1
        return max(attempt, 1)

    def can_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts

    async def schedule(self, message: RabbitMessage, attempt: int) -> str:
        retry_queue = self.topology.retry_queue_for_attempt(attempt)
        await self.broker.publish(
            message.body,
            exchange=self.topology.exchange,
            routing_key=retry_queue.routing_key,
            mandatory=True,
            persist=True,
            timeout=self.timeout,
            message_id=message.message_id,
            message_type=message.raw_message.type,
            content_type=message.content_type,
            headers={Header.ATTEMPT: attempt + 1},
        )
        return retry_queue.name
