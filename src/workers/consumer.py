import asyncio

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy.exc import SQLAlchemyError

from core.config import Settings, get_settings
from core.custom_logging import configure_logging, get_logger
from messaging.broker import build_broker, connect_broker
from messaging.topology import PaymentsTopology
from schemas.pipeline import PipelineOutcome, PipelineResult
from services.gateway import MockPaymentGateway
from services.pipeline import PaymentPipeline
from services.webhooks import WebhookClient
from utils.lifecycle import stop_event_on_signals

logger = get_logger()


class PaymentConsumer:
    def __init__(self, pipeline: PaymentPipeline) -> None:
        self.pipeline = pipeline

    async def handle(self, message: RabbitMessage) -> None:
        try:
            result = await self.pipeline.process(message.body)
        except (SQLAlchemyError, OSError) as exc:
            result = PipelineResult(
                outcome=PipelineOutcome.RETRY, reason=f"{type(exc).__name__}: {exc}"
            )

        fields = {"message_id": message.message_id, "reason": result.reason}
        if result.outcome is PipelineOutcome.DONE:
            await message.ack()
            logger.info("Payment message processed", **fields)
        elif result.outcome is PipelineOutcome.RETRY:
            await message.reject(requeue=False)
            logger.warning("Retry not implemented yet, dead-lettered", **fields)
        else:
            await message.reject(requeue=False)
            logger.error("Payment message dead-lettered", **fields)


def create_consumer(settings: Settings, pipeline: PaymentPipeline) -> RabbitBroker:
    broker = build_broker(settings, prefetch=settings.consumer_prefetch)
    topology = PaymentsTopology(settings)
    consumer = PaymentConsumer(pipeline)
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
