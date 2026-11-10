import pytest
from pydantic import ValidationError

from core.config import Settings

WORKER_VARIABLES = (
    "API_KEY",
    "RABBITMQ_URL",
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_USER",
    "RABBITMQ_PASSWORD",
    "RABBITMQ_VHOST",
    "OUTBOX_BATCH_SIZE",
    "CONSUMER_PREFETCH",
    "GATEWAY_DELAY_MIN",
    "GATEWAY_DELAY_MAX",
    "GATEWAY_SUCCESS_RATE",
    "RETRY_MAX_ATTEMPTS",
    "RETRY_BASE_DELAY_MS",
    "WEBHOOK_SECRET",
    "WEBHOOK_ALLOW_PRIVATE",
    "IDEMPOTENCY_KEY_MAX_LENGTH",
    "PAYMENT_AMOUNT_MAX_DIGITS",
    "PAYMENT_AMOUNT_DECIMAL_PLACES",
    "PAYMENT_DESCRIPTION_MAX_LENGTH",
    "PAYMENT_METADATA_MAX_BYTES",
    "WEBHOOK_URL_MAX_LENGTH",
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for variable in WORKER_VARIABLES:
        monkeypatch.delenv(variable, raising=False)
    return monkeypatch


def test_worker_defaults_match_spec(clean_env: pytest.MonkeyPatch) -> None:
    settings = Settings(_env_file=None)

    assert settings.api_key is None
    assert settings.rabbitmq_url == "amqp://basis:basis@127.0.0.1:5672/%2F"
    assert settings.consumer_prefetch == 10
    assert (settings.gateway_delay_min, settings.gateway_delay_max) == (2.0, 5.0)
    assert settings.gateway_success_rate == 0.9
    assert settings.retry_max_attempts == 3
    assert settings.retry_delays_ms == [2000, 4000]
    assert settings.webhook_allow_private is False


def test_rabbitmq_url_is_built_from_parts_and_quoted(
    clean_env: pytest.MonkeyPatch,
) -> None:
    clean_env.setenv("RABBITMQ_HOST", "rabbitmq")
    clean_env.setenv("RABBITMQ_USER", "user")
    clean_env.setenv("RABBITMQ_PASSWORD", "p@ss/word")
    clean_env.setenv("RABBITMQ_VHOST", "payments")

    settings = Settings(_env_file=None)

    assert settings.rabbitmq_url == "amqp://user:p%40ss%2Fword@rabbitmq:5672/payments"


def test_rabbitmq_url_override_wins(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("RABBITMQ_URL", "amqp://a:b@broker:5673/")
    clean_env.setenv("RABBITMQ_HOST", "ignored")

    assert Settings(_env_file=None).rabbitmq_url == "amqp://a:b@broker:5673/"


def test_secrets_are_not_exposed_in_repr(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("API_KEY", "super-secret")
    clean_env.setenv("WEBHOOK_SECRET", "hmac-secret")

    settings = Settings(_env_file=None)

    assert settings.api_key is not None
    assert settings.api_key.get_secret_value() == "super-secret"
    assert "super-secret" not in repr(settings)
    assert "hmac-secret" not in repr(settings)


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("GATEWAY_SUCCESS_RATE", "1.5"),
        ("GATEWAY_SUCCESS_RATE", "-0.1"),
        ("RETRY_MAX_ATTEMPTS", "0"),
        ("RETRY_BASE_DELAY_MS", "0"),
        ("CONSUMER_PREFETCH", "0"),
        ("OUTBOX_BATCH_SIZE", "5000"),
    ],
)
def test_out_of_range_values_are_rejected(
    clean_env: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    clean_env.setenv(variable, value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_gateway_delay_min_above_max_is_rejected(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("GATEWAY_DELAY_MIN", "6")
    clean_env.setenv("GATEWAY_DELAY_MAX", "5")

    with pytest.raises(ValidationError, match="GATEWAY_DELAY_MIN"):
        Settings(_env_file=None)


def test_single_attempt_means_no_retry_delays(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("RETRY_MAX_ATTEMPTS", "1")

    assert Settings(_env_file=None).retry_delays_ms == []


def test_payment_limit_defaults_match_database_columns(
    clean_env: pytest.MonkeyPatch,
) -> None:
    settings = Settings(_env_file=None)

    assert settings.idempotency_key_max_length == 255
    assert settings.payment_amount_max_digits == 18
    assert settings.payment_amount_decimal_places == 2
    assert settings.payment_description_max_length == 1000
    assert settings.payment_metadata_max_bytes == 16 * 1024
    assert settings.webhook_url_max_length == 2048


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("IDEMPOTENCY_KEY_MAX_LENGTH", "256"),
        ("PAYMENT_AMOUNT_MAX_DIGITS", "19"),
        ("PAYMENT_AMOUNT_DECIMAL_PLACES", "3"),
        ("PAYMENT_DESCRIPTION_MAX_LENGTH", "1001"),
        ("WEBHOOK_URL_MAX_LENGTH", "2049"),
    ],
)
def test_payment_limits_cannot_exceed_database_columns(
    clean_env: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    clean_env.setenv(variable, value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize(
    "url",
    [
        "sqlite:///basis.db",
        "mysql://user:pass@db/basis",
        "postgresql+asyncpg://u:p@db/b",
    ],
)
def test_non_psycopg_postgres_urls_are_rejected(
    clean_env: pytest.MonkeyPatch, url: str
) -> None:
    clean_env.setenv("DATABASE_URL", url)

    with pytest.raises(ValidationError, match="PostgreSQL with the psycopg driver"):
        Settings(_env_file=None)


def test_outbox_relay_defaults(clean_env: pytest.MonkeyPatch) -> None:
    settings = Settings(_env_file=None)

    assert settings.outbox_publish_timeout == 5
    assert settings.outbox_max_backoff_seconds == 60
    assert settings.outbox_alert_after_attempts == 10
    assert settings.outbox_last_error_max_length == 1000
    assert settings.outbox_purge_interval_seconds == 3600
    assert settings.outbox_backlog_log_interval_seconds == 30
    assert settings.rabbitmq_connect_retry_max_seconds == 30
