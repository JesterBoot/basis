import asyncio

from aiormq.exceptions import AMQPConnectionError
from faststream.rabbit import Channel, RabbitBroker
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    RetryError,
    retry_if_exception_type,
    wait_exponential,
)

from core.config import Settings, get_settings
from core.custom_logging import get_logger
from messaging.topology import PaymentsTopology
from utils.lifecycle import sleep_unless_stopped

logger = get_logger()


def build_broker(
    settings: Settings | None = None,
    prefetch: int | None = None,
    graceful_timeout: float | None = None,
) -> RabbitBroker:
    resolved = settings or get_settings()
    return RabbitBroker(
        resolved.rabbitmq_url,
        graceful_timeout=graceful_timeout,
        default_channel=Channel(
            prefetch_count=prefetch, publisher_confirms=True, on_return_raises=True
        ),
    )


def _log_retry(state: RetryCallState) -> None:
    error = state.outcome.exception() if state.outcome else None
    logger.warning(
        "RabbitMQ not reachable, retrying",
        attempt=state.attempt_number,
        error=type(error).__name__,
    )


async def connect_broker(
    broker: RabbitBroker, settings: Settings, stop: asyncio.Event
) -> bool:
    retrying = AsyncRetrying(
        retry=retry_if_exception_type((AMQPConnectionError, OSError)),  # type: ignore
        wait=wait_exponential(
            multiplier=1, max=settings.rabbitmq_connect_retry_max_seconds
        ),
        stop=lambda _: stop.is_set(),
        sleep=lambda seconds: sleep_unless_stopped(stop, seconds),
        before_sleep=_log_retry,
    )
    try:
        async for attempt in retrying:
            with attempt:
                await broker.connect()
                await PaymentsTopology(settings).declare(broker)
    except RetryError:
        return False
    return True
