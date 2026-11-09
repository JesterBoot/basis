from functools import lru_cache

from pydantic import AliasChoices, Field, model_validator
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
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
