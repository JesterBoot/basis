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

    api_key: SecretStr | None = Field(default=None, validation_alias="API_KEY")

    rabbitmq_url_raw: str | None = Field(default=None, validation_alias="RABBITMQ_URL")
    rabbitmq_host: str = Field(default="127.0.0.1", validation_alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, gt=0, validation_alias="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="basis", validation_alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(
        default="basis", validation_alias="RABBITMQ_PASSWORD"
    )
    rabbitmq_vhost: str = Field(default="/", validation_alias="RABBITMQ_VHOST")

    outbox_poll_interval: float = Field(
        default=0.5, gt=0, validation_alias="OUTBOX_POLL_INTERVAL"
    )
    outbox_batch_size: int = Field(
        default=100, gt=0, le=1000, validation_alias="OUTBOX_BATCH_SIZE"
    )
    outbox_retention_days: int = Field(
        default=7, gt=0, validation_alias="OUTBOX_RETENTION_DAYS"
    )

    consumer_prefetch: int = Field(
        default=10, gt=0, validation_alias="CONSUMER_PREFETCH"
    )

    gateway_delay_min: float = Field(
        default=2.0, ge=0, validation_alias="GATEWAY_DELAY_MIN"
    )
    gateway_delay_max: float = Field(
        default=5.0, ge=0, validation_alias="GATEWAY_DELAY_MAX"
    )
    gateway_success_rate: float = Field(
        default=0.9, ge=0, le=1, validation_alias="GATEWAY_SUCCESS_RATE"
    )

    retry_max_attempts: int = Field(
        default=3, ge=1, validation_alias="RETRY_MAX_ATTEMPTS"
    )
    retry_base_delay_ms: int = Field(
        default=2000, gt=0, validation_alias="RETRY_BASE_DELAY_MS"
    )

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
