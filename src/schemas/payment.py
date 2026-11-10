import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from core.config import get_settings
from models.enums import Currency, PaymentStatus, WebhookStatus
from utils.hashing import sha256_hex
from utils.network import is_private_host
from utils.validation import ensure_storable_text, validate_json_tree

_settings = get_settings()


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(
        gt=0,
        max_digits=_settings.payment_amount_max_digits,
        decimal_places=_settings.payment_amount_decimal_places,
    )
    currency: Currency
    description: str = Field(
        min_length=1, max_length=_settings.payment_description_max_length
    )
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("description", mode="before")
    @classmethod
    def strip_description(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value

    @field_validator("description")
    @classmethod
    def description_is_storable(cls, value: str) -> str:
        ensure_storable_text(value, "description")
        return value

    @field_validator("metadata")
    @classmethod
    def metadata_is_storable(cls, value: dict[str, Any]) -> dict[str, Any]:
        settings = get_settings()
        validate_json_tree(value, settings.payment_metadata_max_depth, "metadata")
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
        except ValueError:
            raise ValueError("metadata must not contain NaN or Infinity") from None
        if len(encoded) > settings.payment_metadata_max_bytes:
            raise ValueError(
                f"metadata must not exceed {settings.payment_metadata_max_bytes} bytes"
            )
        return value

    @field_validator("webhook_url")
    @classmethod
    def webhook_url_is_allowed(cls, value: HttpUrl) -> HttpUrl:
        max_length = get_settings().webhook_url_max_length
        if len(str(value)) > max_length:
            raise ValueError(f"webhook_url must not exceed {max_length} characters")
        if (
            value.host
            and is_private_host(value.host)
            and not get_settings().webhook_allow_private
        ):
            raise ValueError("webhook_url must point to a public host")
        return value

    def request_hash(self) -> str:
        places = _settings.payment_amount_decimal_places
        return sha256_hex(
            {
                "amount": format(self.amount.quantize(Decimal(1).scaleb(-places)), "f"),
                "currency": self.currency.value,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": str(self.webhook_url),
            }
        )


class PaymentCreated(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentCreateResult(BaseModel):
    payment: PaymentCreated
    replayed: bool


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="meta")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: HttpUrl
    webhook_status: WebhookStatus
    webhook_attempts: int
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None
