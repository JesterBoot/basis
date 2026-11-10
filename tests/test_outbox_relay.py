import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.config import Settings
from db.session import AsyncSession
from messaging.publisher import EventPublisher
from models import OutboxEvent
from services.outbox import OutboxDataManager, OutboxRelayService
from workers.outbox_relay import OutboxRelay

pytestmark = pytest.mark.database


def relay_service(
    session: AsyncSession, publisher: EventPublisher, batch_size: int
) -> OutboxRelayService:
    settings = Settings(_env_file=None, OUTBOX_BATCH_SIZE=batch_size)
    return OutboxRelayService(session, publisher, settings)


class FakePublisher:
    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.fail_on = fail_on or set()
        self.calls = 0
        self.published: list[uuid.UUID] = []

    async def publish(self, event: OutboxEvent) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise ConnectionError("broker is down")
        self.published.append(event.id)


async def add_events(
    session: AsyncSession, count: int, **overrides: Any
) -> list[OutboxEvent]:
    events = [
        OutboxEvent(
            aggregate_type="payment",
            aggregate_id=uuid.uuid4(),
            event_type="payment.created",
            routing_key="payments.new",
            payload={"n": n},
            **overrides,
        )
        for n in range(count)
    ]
    session.add_all(events)
    await session.commit()
    for offset, event in enumerate(events):
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event.id)
            .values(next_attempt_at=func.now() - timedelta(seconds=count - offset))
        )
    await session.commit()
    return events


async def reload(session: AsyncSession, event_id: uuid.UUID) -> OutboxEvent:
    event = await session.get(OutboxEvent, event_id, populate_existing=True)
    assert event is not None
    return event


async def test_relay_publishes_pending_events_in_order(session: AsyncSession) -> None:
    events = await add_events(session, 3)
    publisher = FakePublisher()

    result = await relay_service(session, publisher, 10).relay_batch()

    assert result.model_dump() == {"claimed": 3, "published": 3, "failed": 0}
    assert publisher.published == [event.id for event in events]
    for event in events:
        assert (await reload(session, event.id)).published_at is not None


async def test_relay_with_nothing_to_do(session: AsyncSession) -> None:
    result = await relay_service(session, FakePublisher(), 10).relay_batch()

    assert result.model_dump() == {"claimed": 0, "published": 0, "failed": 0}


async def test_batch_size_limits_claim(session: AsyncSession) -> None:
    await add_events(session, 5)

    result = await relay_service(session, FakePublisher(), 2).relay_batch()

    assert (result.claimed, result.published) == (2, 2)


async def test_failure_stops_batch_and_reschedules_only_failed_event(
    session: AsyncSession,
) -> None:
    first, second, third = await add_events(session, 3)
    publisher = FakePublisher(fail_on={2})

    result = await relay_service(session, publisher, 10).relay_batch()

    assert result.model_dump() == {"claimed": 3, "published": 1, "failed": 1}
    assert (await reload(session, first.id)).published_at is not None
    failed = await reload(session, second.id)
    assert failed.published_at is None
    assert failed.attempts == 1
    assert failed.last_error == "ConnectionError: broker is down"
    untouched = await reload(session, third.id)
    assert (untouched.published_at, untouched.attempts) == (None, 0)


async def test_rescheduled_event_waits_for_backoff(session: AsyncSession) -> None:
    (event,) = await add_events(session, 1)
    await relay_service(session, FakePublisher(fail_on={1}), 10).relay_batch()

    retry = await relay_service(session, FakePublisher(), 10).relay_batch()

    assert retry.claimed == 0
    assert (await reload(session, event.id)).published_at is None


@pytest.mark.parametrize(
    ("attempts_before", "expected_delay"),
    [(0, 2), (1, 4), (4, 32), (5, 60), (6, 60), (5000, 60)],
)
async def test_backoff_doubles_and_is_capped_without_overflow(
    session: AsyncSession, attempts_before: int, expected_delay: int
) -> None:
    (event,) = await add_events(session, 1, attempts=attempts_before)

    attempts = await OutboxDataManager(session).schedule_retry(event.id, "boom", 60)
    delay = await session.scalar(
        select(OutboxEvent.next_attempt_at - func.now()).where(
            OutboxEvent.id == event.id
        )
    )
    await session.commit()

    assert attempts == attempts_before + 1
    assert delay == timedelta(seconds=expected_delay)


async def test_long_error_messages_are_truncated(session: AsyncSession) -> None:
    (event,) = await add_events(session, 1)

    class ExplodingPublisher:
        async def publish(self, _: OutboxEvent) -> None:
            raise RuntimeError("x" * 5000)

    await relay_service(session, ExplodingPublisher(), 10).relay_batch()

    stored = await reload(session, event.id)
    assert stored.last_error is not None
    assert len(stored.last_error) == 1000


async def test_two_relays_never_claim_the_same_event(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await add_events(session, 6)
    first_claimed = asyncio.Event()
    release_first = asyncio.Event()

    class BlockingPublisher(FakePublisher):
        async def publish(self, event: OutboxEvent) -> None:
            first_claimed.set()
            await release_first.wait()
            await super().publish(event)

    blocking, fast = BlockingPublisher(), FakePublisher()

    async def run(publisher: FakePublisher) -> None:
        async with session_factory() as own_session:
            await relay_service(own_session, publisher, 3).relay_batch()

    first = asyncio.create_task(run(blocking))
    await first_claimed.wait()
    await run(fast)
    release_first.set()
    await first

    assert len(blocking.published) == 3
    assert len(fast.published) == 3
    assert not set(blocking.published) & set(fast.published)
    pending = await session.scalar(
        select(func.count())
        .select_from(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
    )
    assert pending == 0


async def test_crash_before_commit_republishes_event(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    (event,) = await add_events(session, 1)
    publisher = FakePublisher()

    async def crash(*_: object) -> None:
        raise OperationalError("UPDATE", {}, Exception("connection lost"))

    with monkeypatch.context() as patched:
        patched.setattr(OutboxDataManager, "mark_published", crash)
        with pytest.raises(OperationalError):
            await relay_service(session, publisher, 10).relay_batch()

    await relay_service(session, publisher, 10).relay_batch()

    assert publisher.published == [event.id, event.id]
    assert (await reload(session, event.id)).published_at is not None


async def test_purge_deletes_only_old_published_events(session: AsyncSession) -> None:
    old, recent, pending = await add_events(session, 3)
    now = datetime.now(UTC)
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id == old.id)
        .values(published_at=now - timedelta(days=8))
    )
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id == recent.id)
        .values(published_at=now - timedelta(days=1))
    )
    await session.commit()
    service = relay_service(session, FakePublisher(), 10)

    deleted = await service.purge_published()

    assert deleted == 1
    remaining = set((await session.scalars(select(OutboxEvent.id))).all())
    assert remaining == {recent.id, pending.id}


async def test_backlog_counts_unpublished_events(session: AsyncSession) -> None:
    await add_events(session, 4)
    service = relay_service(session, FakePublisher(), 2)
    await service.relay_batch()

    assert await service.backlog() == 2


async def test_relay_loop_drains_backlog_and_writes_heartbeat(
    session: AsyncSession, tmp_path: Path
) -> None:
    await add_events(session, 5)
    heartbeat = tmp_path / "relay.heartbeat"
    settings = Settings(
        _env_file=None,
        OUTBOX_BATCH_SIZE=2,
        OUTBOX_POLL_INTERVAL=0.05,
        OUTBOX_HEARTBEAT_PATH=str(heartbeat),
    )
    publisher = FakePublisher()
    relay = OutboxRelay(settings, publisher)
    stop = asyncio.Event()

    task = asyncio.create_task(relay.run(stop))
    for _ in range(100):
        if len(publisher.published) == 5:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert len(publisher.published) == 5
    assert heartbeat.exists()


async def test_relay_loop_survives_database_errors(
    session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings(
        _env_file=None,
        OUTBOX_POLL_INTERVAL=0.01,
        OUTBOX_HEARTBEAT_PATH=str(tmp_path / "hb"),
    )
    relay = OutboxRelay(settings, FakePublisher())
    calls = 0
    stop = asyncio.Event()

    async def flaky_run_once() -> None:
        nonlocal calls
        calls += 1
        if calls >= 3:
            stop.set()
        raise OperationalError("SELECT", {}, Exception("db down"))

    monkeypatch.setattr(relay, "run_once", flaky_run_once)

    await asyncio.wait_for(relay.run(stop), timeout=2)

    assert calls >= 3
    assert (tmp_path / "hb").exists()
