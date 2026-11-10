import random
import uuid
from collections import Counter

import pytest

from core.config import Settings
from models import PaymentStatus
from services.gateway import MockPaymentGateway


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def gateway_settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


def make_gateway(
    settings: Settings | None = None, seed: int = 42
) -> tuple[MockPaymentGateway, RecordingSleep]:
    sleep = RecordingSleep()
    gateway = MockPaymentGateway(
        settings or gateway_settings(), rng=random.Random(seed), sleep=sleep
    )
    return gateway, sleep


async def charge_many(
    gateway: MockPaymentGateway, count: int
) -> Counter[PaymentStatus]:
    return Counter([await gateway.charge(uuid.uuid4()) for _ in range(count)])


async def test_delay_stays_within_configured_bounds() -> None:
    gateway, sleep = make_gateway()

    await charge_many(gateway, 500)

    assert len(sleep.delays) == 500
    assert all(2.0 <= delay <= 5.0 for delay in sleep.delays)
    assert min(sleep.delays) < 2.1
    assert max(sleep.delays) > 4.9


async def test_fixed_delay_when_min_equals_max() -> None:
    settings = gateway_settings(GATEWAY_DELAY_MIN=3, GATEWAY_DELAY_MAX=3)
    gateway, sleep = make_gateway(settings)

    await charge_many(gateway, 10)

    assert sleep.delays == [3.0] * 10


async def test_zero_delay_settings_do_not_wait() -> None:
    settings = gateway_settings(GATEWAY_DELAY_MIN=0, GATEWAY_DELAY_MAX=0)
    gateway = MockPaymentGateway(settings)

    assert await gateway.charge(uuid.uuid4()) in {
        PaymentStatus.SUCCEEDED,
        PaymentStatus.FAILED,
    }


@pytest.mark.parametrize(
    ("success_rate", "expected"),
    [(1.0, PaymentStatus.SUCCEEDED), (0.0, PaymentStatus.FAILED)],
)
async def test_extreme_success_rates_are_deterministic(
    success_rate: float, expected: PaymentStatus
) -> None:
    gateway, _ = make_gateway(gateway_settings(GATEWAY_SUCCESS_RATE=success_rate))

    outcomes = await charge_many(gateway, 200)

    assert outcomes == Counter({expected: 200})


async def test_default_success_rate_is_about_ninety_percent() -> None:
    gateway, _ = make_gateway(seed=2026)

    outcomes = await charge_many(gateway, 10_000)

    assert set(outcomes) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
    assert 0.88 <= outcomes[PaymentStatus.SUCCEEDED] / 10_000 <= 0.92


async def test_never_returns_pending() -> None:
    gateway, _ = make_gateway(gateway_settings(GATEWAY_SUCCESS_RATE=0.5))

    outcomes = await charge_many(gateway, 1_000)

    assert PaymentStatus.PENDING not in outcomes


async def test_same_seed_gives_same_results() -> None:
    first, first_sleep = make_gateway(seed=7)
    second, second_sleep = make_gateway(seed=7)
    payment_id = uuid.uuid4()

    first_outcomes = [await first.charge(payment_id) for _ in range(50)]
    second_outcomes = [await second.charge(payment_id) for _ in range(50)]

    assert first_outcomes == second_outcomes
    assert first_sleep.delays == second_sleep.delays
