import asyncio
from pathlib import Path
from typing import Any

import pytest

from core.config import Settings
from messaging.broker import connect_broker
from messaging.topology import PaymentsTopology
from utils.lifecycle import Every, run_heartbeat, sleep_unless_stopped


async def test_sleep_returns_early_when_stopped() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(0.02, stop.set)
    started = loop.time()

    await sleep_unless_stopped(stop, 5)

    assert loop.time() - started < 1


async def test_sleep_waits_full_time_when_not_stopped() -> None:
    loop = asyncio.get_running_loop()
    started = loop.time()

    await sleep_unless_stopped(asyncio.Event(), 0.05)

    assert loop.time() - started >= 0.05


async def test_every_is_due_once_per_interval() -> None:
    every = Every(0.05)

    assert every.due() is True
    assert every.due() is False
    await asyncio.sleep(0.06)
    assert every.due() is True


class FlakyBroker:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.connect_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_calls <= self.failures:
            raise ConnectionRefusedError("rabbitmq down")


@pytest.fixture
def no_topology(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    declared: list[Any] = []

    async def fake_declare(topology: PaymentsTopology, broker: Any) -> None:
        declared.append(broker)

    monkeypatch.setattr(PaymentsTopology, "declare", fake_declare)
    return declared


FAST_RETRY = Settings(_env_file=None, RABBITMQ_CONNECT_RETRY_MAX_SECONDS=0.01)


async def test_connect_broker_retries_until_success(no_topology: list[Any]) -> None:
    broker = FlakyBroker(failures=3)

    connected = await connect_broker(broker, FAST_RETRY, asyncio.Event())  # type: ignore[arg-type]

    assert connected is True
    assert broker.connect_calls == 4
    assert no_topology == [broker]


async def test_connect_broker_gives_up_when_stopped(no_topology: list[Any]) -> None:
    broker = FlakyBroker(failures=1000)
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, stop.set)

    connected = await asyncio.wait_for(
        connect_broker(broker, FAST_RETRY, stop),  # type: ignore[arg-type]
        timeout=2,
    )

    assert connected is False
    assert no_topology == []


async def test_connect_broker_does_not_retry_unexpected_errors(
    no_topology: list[Any],
) -> None:
    class BrokenBroker:
        async def connect(self) -> None:
            raise ValueError("bad url")

    with pytest.raises(ValueError, match="bad url"):
        await connect_broker(BrokenBroker(), FAST_RETRY, asyncio.Event())  # type: ignore[arg-type]


async def test_heartbeat_touches_file_while_healthy(tmp_path: Path) -> None:
    path = tmp_path / "hb"
    stop = asyncio.Event()
    checks = 0

    async def healthy() -> bool:
        nonlocal checks
        checks += 1
        if checks >= 3:
            stop.set()
        return True

    await asyncio.wait_for(run_heartbeat(path, 0.01, stop, healthy), timeout=2)

    assert path.exists()
    assert checks == 3


async def test_heartbeat_skips_touch_when_unhealthy(tmp_path: Path) -> None:
    path = tmp_path / "hb"
    stop = asyncio.Event()

    async def unhealthy() -> bool:
        stop.set()
        return False

    await asyncio.wait_for(run_heartbeat(path, 0.01, stop, unhealthy), timeout=2)

    assert not path.exists()


async def test_heartbeat_stops_promptly(tmp_path: Path) -> None:
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, stop.set)

    async def healthy() -> bool:
        return True

    await asyncio.wait_for(run_heartbeat(tmp_path / "hb", 60, stop, healthy), timeout=1)
