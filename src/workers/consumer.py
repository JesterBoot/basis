import asyncio

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from core.config import Settings, get_settings
from core.custom_logging import configure_logging, get_logger
from messaging.broker import build_broker, connect_broker
from messaging.retry import RetryScheduler
from messaging.topology import PaymentsTopology
from schemas.pipeline import PipelineOutcome, PipelineResult
from services.gateway import MockPaymentGateway
from services.pipeline import PaymentPipeline
from services.webhooks import WebhookClient
from utils.lifecycle import stop_event_on_signals

logger = get_logger()


class PaymentConsumer:
    def __init__(self, pipeline: PaymentPipeline, retries: RetryScheduler) -> None:
        self.pipeline = pipeline
        self.retries = retries

    async def handle(self, message: RabbitMessage) -> None:
        attempt = self.retries.attempt_of(message.headers)
        try:
            result = await self.pipeline.process(message.body)
        except Exception as exc:  # noqa: BLE001
            result = PipelineResult(
                outcome=PipelineOutcome.RETRY, reason=f"{type(exc).__name__}: {exc}"
            )

        fields = {
            "message_id": message.message_id,
            "attempt": attempt,
            "reason": result.reason,
        }
        if result.outcome is PipelineOutcome.DONE:
            await message.ack()
            logger.info("Payment message processed", **fields)
        elif result.outcome is PipelineOutcome.DEAD_LETTER:
            await message.reject(requeue=False)
            logger.error("Payment message dead-lettered", **fields)
        elif self.retries.can_retry(attempt):
            await self._retry(message, attempt, fields)
        else:
            await self._give_up(message, result, fields)

    async def _retry(
        self, message: RabbitMessage, attempt: int, fields: dict[str, object]
    ) -> None:
        try:
            retry_queue = await self.retries.schedule(message, attempt)
        except Exception as exc:  # noqa: BLE001
            await message.nack(requeue=True)
            logger.error(
                "Could not schedule retry, requeued", error=type(exc).__name__, **fields
            )
            return
        await message.ack()
        logger.warning(
            "Payment message scheduled for retry", queue=retry_queue, **fields
        )

    async def _give_up(
        self, message: RabbitMessage, result: PipelineResult, fields: dict[str, object]
    ) -> None:
        if result.payment_id is not None:
            await self.pipeline.give_up(result.payment_id)
        await message.reject(requeue=False)
        logger.error("Retries exhausted, payment message dead-lettered", **fields)


def create_consumer(settings: Settings, pipeline: PaymentPipeline) -> RabbitBroker:
    broker = build_broker(settings, prefetch=settings.consumer_prefetch)
    topology = PaymentsTopology(settings)
    consumer = PaymentConsumer(pipeline, RetryScheduler(broker, settings))
    broker.subscriber(
        topology.new_queue, topology.exchange, ack_policy=AckPolicy.MANUAL
    )(consumer.handle)
    return broker


async def main() -> None:
    configure_logging()
    settings = get_settings()
    stop = stop_event_on_signals()
    async with WebhookClient.from_settings(settings) as webhook_client:
        pipeline = PaymentPipeline(
            MockPaymentGateway(settings), webhook_client, settings
        )
        broker = create_consumer(settings, pipeline)
        try:
            if await connect_broker(broker, settings, stop):
                await broker.start()
                logger.info(
                    "Payment consumer started", prefetch=settings.consumer_prefetch
                )
                await stop.wait()
        finally:
            await broker.stop()
            logger.info("Payment consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
