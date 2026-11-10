import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from core.config import Settings
from messaging.retry import RetryScheduler
from schemas.pipeline import PipelineOutcome, PipelineResult
from workers.consumer import PaymentConsumer


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        (None, 1),
        ({}, 1),
        ({"x-attempt": 2}, 2),
        ({"x-attempt": "3"}, 3),
        ({"x-attempt": "abc"}, 1),
        ({"x-attempt": None}, 1),
        ({"x-attempt": 0}, 1),
        ({"x-attempt": -5}, 1),
    ],
)
def test_attempt_header_parsing(headers: dict[str, Any] | None, expected: int) -> None:
    assert RetryScheduler.attempt_of(headers) == expected


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict[str, Any]] = []

    async def publish(self, body: bytes, **kwargs: Any) -> None:
        if self.fail:
            raise ConnectionError("broker down")
        self.published.append({"body": body, **kwargs})


class FakeMessage:
    def __init__(self, events: list[str], attempt: int | None = None) -> None:
        self.events = events
        self.body = b'{"payload": true}'
        self.message_id = str(uuid.uuid4())
        self.content_type = "application/json"
        self.headers = {} if attempt is None else {"x-attempt": attempt}
        self.raw_message = SimpleNamespace(type="payment.created")

    async def ack(self) -> None:
        self.events.append("ack")

    async def reject(self, requeue: bool = False) -> None:
        self.events.append(f"reject(requeue={requeue})")

    async def nack(self, requeue: bool = True) -> None:
        self.events.append(f"nack(requeue={requeue})")


class FakePipeline:
    def __init__(self, events: list[str], result: PipelineResult | Exception) -> None:
        self.events = events
        self.result = result
        self.given_up: list[uuid.UUID] = []

    async def process(self, body: bytes) -> PipelineResult:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    async def give_up(self, payment_id: uuid.UUID) -> None:
        self.events.append("give_up")
        self.given_up.append(payment_id)


class RecordingScheduler(RetryScheduler):
    def __init__(
        self, events: list[str], broker: FakeBroker, settings: Settings
    ) -> None:
        super().__init__(broker, settings)  # type: ignore[arg-type]
        self.events = events

    async def schedule(self, message: Any, attempt: int) -> str:
        queue = await super().schedule(message, attempt)
        self.events.append(f"schedule({attempt})")
        return queue


SETTINGS = Settings(_env_file=None, RETRY_MAX_ATTEMPTS=3, RETRY_BASE_DELAY_MS=2000)


def make_consumer(
    result: PipelineResult | Exception, broker: FakeBroker | None = None
) -> tuple[PaymentConsumer, FakePipeline, FakeBroker, list[str]]:
    events: list[str] = []
    fake_broker = broker or FakeBroker()
    pipeline = FakePipeline(events, result)
    scheduler = RecordingScheduler(events, fake_broker, SETTINGS)
    return PaymentConsumer(pipeline, scheduler), pipeline, fake_broker, events  # type: ignore[arg-type]


RETRY = PipelineResult(outcome=PipelineOutcome.RETRY, reason="HTTP 503")


async def test_done_is_acked() -> None:
    consumer, _, broker, events = make_consumer(
        PipelineResult(outcome=PipelineOutcome.DONE)
    )

    await consumer.handle(FakeMessage(events))  # type: ignore[arg-type]

    assert events == ["ack"]
    assert broker.published == []


async def test_dead_letter_is_rejected_without_requeue() -> None:
    consumer, _, _, events = make_consumer(
        PipelineResult(outcome=PipelineOutcome.DEAD_LETTER, reason="bad")
    )

    await consumer.handle(FakeMessage(events))  # type: ignore[arg-type]

    assert events == ["reject(requeue=False)"]


@pytest.mark.parametrize(
    ("attempt", "queue"),
    [
        (None, "payments.new.retry.2000ms"),
        (1, "payments.new.retry.2000ms"),
        (2, "payments.new.retry.4000ms"),
    ],
)
async def test_retry_republishes_then_acks(attempt: int | None, queue: str) -> None:
    consumer, _, broker, events = make_consumer(RETRY)
    message = FakeMessage(events, attempt)

    await consumer.handle(message)  # type: ignore[arg-type]

    current = attempt or 1
    assert events == [f"schedule({current})", "ack"]
    (published,) = broker.published
    assert published["routing_key"] == queue
    assert published["headers"] == {"x-attempt": current + 1}
    assert published["message_id"] == message.message_id
    assert published["body"] == message.body
    assert published["message_type"] == "payment.created"
    assert published["persist"] is True
    assert published["mandatory"] is True


async def test_last_attempt_gives_up_and_dead_letters() -> None:
    payment_id = uuid.uuid4()
    consumer, pipeline, broker, events = make_consumer(
        PipelineResult(
            outcome=PipelineOutcome.RETRY, reason="HTTP 503", payment_id=payment_id
        )
    )

    await consumer.handle(FakeMessage(events, attempt=3))  # type: ignore[arg-type]

    assert events == ["give_up", "reject(requeue=False)"]
    assert pipeline.given_up == [payment_id]
    assert broker.published == []


async def test_last_attempt_without_payment_does_not_touch_webhook() -> None:
    consumer, pipeline, _, events = make_consumer(RETRY)

    await consumer.handle(FakeMessage(events, attempt=3))  # type: ignore[arg-type]

    assert events == ["reject(requeue=False)"]
    assert pipeline.given_up == []


async def test_failed_retry_publish_requeues_original() -> None:
    consumer, _, _, events = make_consumer(RETRY, broker=FakeBroker(fail=True))

    await consumer.handle(FakeMessage(events, attempt=1))  # type: ignore[arg-type]

    assert events == ["nack(requeue=True)"]


@pytest.mark.parametrize(
    "error", [RuntimeError("bug"), ConnectionError("db down"), ValueError("oops")]
)
async def test_unexpected_pipeline_errors_are_retried(error: Exception) -> None:
    consumer, _, broker, events = make_consumer(error)

    await consumer.handle(FakeMessage(events, attempt=1))  # type: ignore[arg-type]

    assert events == ["schedule(1)", "ack"]
    assert len(broker.published) == 1


async def test_single_attempt_setting_never_retries() -> None:
    events: list[str] = []
    settings = Settings(_env_file=None, RETRY_MAX_ATTEMPTS=1)
    pipeline = FakePipeline(events, RETRY)
    scheduler = RecordingScheduler(events, FakeBroker(), settings)
    consumer = PaymentConsumer(pipeline, scheduler)  # type: ignore[arg-type]

    await consumer.handle(FakeMessage(events))  # type: ignore[arg-type]

    assert events == ["reject(requeue=False)"]
