import asyncio
import signal
from collections.abc import Awaitable, Callable
from pathlib import Path


def stop_event_on_signals(
    signals: tuple[signal.Signals, ...] = (signal.SIGTERM, signal.SIGINT),
) -> asyncio.Event:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    return stop


async def sleep_unless_stopped(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


class Every:
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self._last = float("-inf")

    def due(self) -> bool:
        now = asyncio.get_running_loop().time()
        if now - self._last < self.seconds:
            return False
        self._last = now
        return True


async def run_heartbeat(
    path: Path,
    interval: float,
    stop: asyncio.Event,
    is_healthy: Callable[[], Awaitable[bool]],
) -> None:
    while not stop.is_set():
        if await is_healthy():
            path.touch()
        await sleep_unless_stopped(stop, interval)
