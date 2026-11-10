import argparse
import asyncio
import json

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from pydantic import BaseModel, ValidationError

from core.config import get_settings
from core.custom_logging import configure_logging, get_logger
from db.session import tasks_session_context
from messaging.names import Exchange, Header, Queue, RoutingKey
from schemas.events import PaymentCreatedEvent
from services.payments import PaymentService

logger = get_logger()


class DeadLetter(BaseModel):
    message_id: str | None
    payment_id: str | None
    attempt: int | None
    reason: str | None
    valid: bool


class ReplaySummary(BaseModel):
    replayed: int
    reopened_webhooks: int
    skipped_invalid: int


class DeadLetterReplayer:
    def __init__(self, channel: AbstractChannel) -> None:
        self.channel = channel

    async def peek(self, limit: int) -> list[DeadLetter]:
        held = await self._take(limit)
        try:
            return [self._describe(message) for message in held]
        finally:
            for message in held:
                await message.nack(requeue=True)

    async def replay(self, limit: int) -> ReplaySummary:
        exchange = await self.channel.get_exchange(Exchange.PAYMENTS)
        held = await self._take(limit)
        replayed = reopened = 0
        invalid: list[AbstractIncomingMessage] = []
        try:
            for message in held:
                event = self._event(message)
                if event is None:
                    invalid.append(message)
                    continue
                async with tasks_session_context() as session:
                    if await PaymentService(session).reopen_webhook(event.payment_id):
                        reopened += 1
                await exchange.publish(
                    self._fresh_copy(message), RoutingKey.PAYMENTS_NEW
                )
                await message.ack()
                replayed += 1
        finally:
            for message in held:
                if not message.processed:
                    await message.nack(requeue=True)
        return ReplaySummary(
            replayed=replayed, reopened_webhooks=reopened, skipped_invalid=len(invalid)
        )

    async def _take(self, limit: int) -> list[AbstractIncomingMessage]:
        queue = await self.channel.get_queue(Queue.PAYMENTS_DLQ)
        held: list[AbstractIncomingMessage] = []
        for _ in range(limit):
            message = await queue.get(no_ack=False, fail=False)
            if message is None:
                break
            held.append(message)
        return held

    @staticmethod
    def _event(message: AbstractIncomingMessage) -> PaymentCreatedEvent | None:
        try:
            return PaymentCreatedEvent.model_validate_json(message.body)
        except ValidationError:
            return None

    @classmethod
    def _describe(cls, message: AbstractIncomingMessage) -> DeadLetter:
        event = cls._event(message)
        deaths = (message.headers or {}).get("x-death") or [{}]
        attempt = (message.headers or {}).get(Header.ATTEMPT)
        return DeadLetter(
            message_id=message.message_id,
            payment_id=str(event.payment_id) if event else None,
            attempt=int(attempt) if isinstance(attempt, int) else None,
            reason=deaths[0].get("reason"),
            valid=event is not None,
        )

    @staticmethod
    def _fresh_copy(message: AbstractIncomingMessage) -> aio_pika.Message:
        return aio_pika.Message(
            message.body,
            message_id=message.message_id,
            type=message.type,
            content_type=message.content_type,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={Header.ATTEMPT: 1},
        )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay dead-lettered payment messages"
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    configure_logging()
    settings = get_settings()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        replayer = DeadLetterReplayer(channel)
        if args.dry_run:
            for dead_letter in await replayer.peek(args.limit):
                print(dead_letter.model_dump_json())
            return
        print(json.dumps((await replayer.replay(args.limit)).model_dump()))


if __name__ == "__main__":
    asyncio.run(main())
