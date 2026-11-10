from functools import lru_cache
from urllib.parse import quote

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    # App
    debug: bool = Field(
        default=False,
        validation_alias="DEBUG",
    )

    # DB
    db_echo: bool = Field(
        default=False,
        validation_alias="DB_ECHO",
    )
    database_url_raw: str | None = Field(
        default=None,
        validation_alias="DATABASE_URL",
    )
    db_host: str = Field(
        default="127.0.0.1",
        validation_alias=AliasChoices("DB_HOST", "POSTGRES_HOST"),
    )
    db_port: int = Field(
        default=5432,
        gt=0,
        validation_alias=AliasChoices("DB_PORT", "POSTGRES_PORT"),
    )
    db_name: str = Field(
        default="basis",
        validation_alias=AliasChoices("DB_NAME", "POSTGRES_DB"),
    )
    db_user: str = Field(
        default="basis",
        validation_alias=AliasChoices("DB_USER", "POSTGRES_USER"),
    )
    db_pass: str = Field(
        default="basis",
        validation_alias=AliasChoices("DB_PASSWORD", "POSTGRES_PASSWORD"),
    )
    db_pool_size: int = Field(default=5, gt=0)
    db_max_overflow: int = Field(default=10, ge=0)
    db_pool_timeout: int = Field(default=30, gt=0)
    db_pool_recycle: int = Field(default=1_800, gt=0)
    db_pool_pre_ping: bool = True

    # API
    api_key: SecretStr | None = Field(default=None, validation_alias="API_KEY")

    # Payment validation
    idempotency_key_pattern: str = Field(
        default=r"^[A-Za-z0-9_\-:.]+$", validation_alias="IDEMPOTENCY_KEY_PATTERN"
    )
    idempotency_key_max_length: int = Field(
        default=255, gt=0, le=255, validation_alias="IDEMPOTENCY_KEY_MAX_LENGTH"
    )
    payment_amount_max_digits: int = Field(
        default=18, gt=2, le=18, validation_alias="PAYMENT_AMOUNT_MAX_DIGITS"
    )
    payment_amount_decimal_places: int = Field(
        default=2, ge=0, le=2, validation_alias="PAYMENT_AMOUNT_DECIMAL_PLACES"
    )
    payment_description_max_length: int = Field(
        default=1000, gt=0, le=1000, validation_alias="PAYMENT_DESCRIPTION_MAX_LENGTH"
    )
    payment_metadata_max_bytes: int = Field(
        default=16 * 1024, gt=0, validation_alias="PAYMENT_METADATA_MAX_BYTES"
    )
    payment_metadata_max_depth: int = Field(
        default=32, gt=0, le=64, validation_alias="PAYMENT_METADATA_MAX_DEPTH"
    )
    webhook_url_max_length: int = Field(
        default=2048, gt=0, le=2048, validation_alias="WEBHOOK_URL_MAX_LENGTH"
    )

    # RabbitMQ
    rabbitmq_url_raw: str | None = Field(default=None, validation_alias="RABBITMQ_URL")
    rabbitmq_host: str = Field(default="127.0.0.1", validation_alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, gt=0, validation_alias="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="basis", validation_alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(
        default="basis", validation_alias="RABBITMQ_PASSWORD"
    )
    rabbitmq_vhost: str = Field(default="/", validation_alias="RABBITMQ_VHOST")
    rabbitmq_connect_retry_max_seconds: float = Field(
        default=30, gt=0, validation_alias="RABBITMQ_CONNECT_RETRY_MAX_SECONDS"
    )

    # Outbox relay
    outbox_poll_interval: float = Field(
        default=0.5, gt=0, validation_alias="OUTBOX_POLL_INTERVAL"
    )
    outbox_batch_size: int = Field(
        default=100, gt=0, le=1000, validation_alias="OUTBOX_BATCH_SIZE"
    )
    outbox_retention_days: int = Field(
        default=7, gt=0, validation_alias="OUTBOX_RETENTION_DAYS"
    )
    outbox_publish_timeout: float = Field(
        default=5.0, gt=0, validation_alias="OUTBOX_PUBLISH_TIMEOUT"
    )
    outbox_heartbeat_path: str = Field(
        default="/tmp/outbox-relay.heartbeat", validation_alias="OUTBOX_HEARTBEAT_PATH"
    )
    outbox_max_backoff_seconds: int = Field(
        default=60, gt=0, le=86_400, validation_alias="OUTBOX_MAX_BACKOFF_SECONDS"
    )
    outbox_alert_after_attempts: int = Field(
        default=10, gt=0, validation_alias="OUTBOX_ALERT_AFTER_ATTEMPTS"
    )
    outbox_last_error_max_length: int = Field(
        default=1000, gt=0, le=10_000, validation_alias="OUTBOX_LAST_ERROR_MAX_LENGTH"
    )
    outbox_purge_interval_seconds: float = Field(
        default=3600, gt=0, validation_alias="OUTBOX_PURGE_INTERVAL_SECONDS"
    )
    outbox_backlog_log_interval_seconds: float = Field(
        default=30, gt=0, validation_alias="OUTBOX_BACKLOG_LOG_INTERVAL_SECONDS"
    )

    # Consumer
    consumer_prefetch: int = Field(
        default=10, gt=0, validation_alias="CONSUMER_PREFETCH"
    )

    # Mock payment gateway
    gateway_delay_min: float = Field(
        default=2.0, ge=0, validation_alias="GATEWAY_DELAY_MIN"
    )
    gateway_delay_max: float = Field(
        default=5.0, ge=0, validation_alias="GATEWAY_DELAY_MAX"
    )
    gateway_success_rate: float = Field(
        default=0.9, ge=0, le=1, validation_alias="GATEWAY_SUCCESS_RATE"
    )

    # Retry and DLQ
    retry_max_attempts: int = Field(
        default=3, ge=1, validation_alias="RETRY_MAX_ATTEMPTS"
    )
    retry_base_delay_ms: int = Field(
        default=2000, gt=0, validation_alias="RETRY_BASE_DELAY_MS"
    )

    # Webhooks
    webhook_timeout: float = Field(
        default=5.0, gt=0, validation_alias="WEBHOOK_TIMEOUT"
    )
    webhook_secret: SecretStr | None = Field(
        default=None, validation_alias="WEBHOOK_SECRET"
    )
    webhook_allow_private: bool = Field(
        default=False, validation_alias="WEBHOOK_ALLOW_PRIVATE"
    )

    @property
    def rabbitmq_url(self) -> str:
        if self.rabbitmq_url_raw:
            return self.rabbitmq_url_raw
        user = quote(self.rabbitmq_user, safe="")
        password = quote(self.rabbitmq_password, safe="")
        vhost = quote(self.rabbitmq_vhost, safe="")
        return f"amqp://{user}:{password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"

    @property
    def retry_delays_ms(self) -> list[int]:
        return [
            self.retry_base_delay_ms * 2 ** (attempt - 1)
            for attempt in range(1, self.retry_max_attempts)
        ]

    @property
    def database_url(self) -> str:
        if self.database_url_raw:
            url = self.database_url_raw
            if url.startswith("postgresql://"):
                url = url.replace("postgresql://", "postgresql+psycopg://", 1)
            return url
        return (
            f"postgresql+psycopg://{self.db_user}:{self.db_pass}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @model_validator(mode="after")
    def validate_limits(self) -> "Settings":
        if not self.database_url.startswith(("postgresql://", "postgresql+psycopg://")):
            raise ValueError("DATABASE_URL must use PostgreSQL with the psycopg driver")
        if self.gateway_delay_min > self.gateway_delay_max:
            raise ValueError("GATEWAY_DELAY_MIN must not exceed GATEWAY_DELAY_MAX")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
