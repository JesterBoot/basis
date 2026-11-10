import asyncio
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from core.config import Settings
from db.session import AsyncSession
from tests.support.outbox import FakePublisher, add_events
from workers.outbox_relay import OutboxRelay

pytestmark = pytest.mark.database


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
