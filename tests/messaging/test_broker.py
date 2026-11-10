import asyncio
from typing import Any

import pytest

from core.config import Settings
from messaging.broker import connect_broker
from messaging.topology import PaymentsTopology


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
