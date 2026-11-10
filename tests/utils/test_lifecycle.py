import asyncio
from pathlib import Path

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
