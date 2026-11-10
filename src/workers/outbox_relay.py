import asyncio
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

from core.config import Settings, get_settings
from core.custom_logging import configure_logging, get_logger
from db.session import tasks_session_context
from messaging.broker import build_broker, connect_broker
from messaging.publisher import EventPublisher, RabbitEventPublisher
from schemas.outbox import OutboxRelayResult
from services.outbox import OutboxRelayService
from utils.lifecycle import Every, sleep_unless_stopped, stop_event_on_signals

logger = get_logger()


class OutboxRelay:
    def __init__(self, settings: Settings, publisher: EventPublisher) -> None:
        self.settings = settings
        self.publisher = publisher
        self.heartbeat = Path(settings.outbox_heartbeat_path)
        self.purge_due = Every(settings.outbox_purge_interval_seconds)
        self.backlog_due = Every(settings.outbox_backlog_log_interval_seconds)

    async def run_once(self) -> OutboxRelayResult:
        async with tasks_session_context() as session:
            service = OutboxRelayService(session, self.publisher, self.settings)
            result = await service.relay_batch()
            if self.purge_due.due():
                await service.purge_published()
            if self.backlog_due.due():
                logger.info("Outbox backlog", unpublished=await service.backlog())
        return result

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("Outbox relay started", batch_size=self.settings.outbox_batch_size)
        while not stop.is_set():
            idle = True
            try:
                result = await self.run_once()
                idle = bool(result.failed) or (
                    result.claimed < self.settings.outbox_batch_size
                )
            except (SQLAlchemyError, OSError) as exc:
                logger.error("Outbox relay iteration failed", error=type(exc).__name__)
            self.heartbeat.touch()
            if idle:
                await sleep_unless_stopped(stop, self.settings.outbox_poll_interval)
        logger.info("Outbox relay stopped")


async def main() -> None:
    configure_logging()
    settings = get_settings()
    stop = stop_event_on_signals()
    broker = build_broker(settings)
    try:
        if await connect_broker(broker, settings, stop):
            publisher = RabbitEventPublisher(broker, settings.outbox_publish_timeout)
            await OutboxRelay(settings, publisher).run(stop)
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
