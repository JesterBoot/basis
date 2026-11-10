import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CHAR,
    CheckConstraint,
    DateTime,
    Enum,
    Integer,
    Numeric,
    String,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base
from models.enums import Currency, PaymentStatus, WebhookStatus


def _string_enum(enum_class: type[StrEnum], name: str, length: int) -> Enum:
    return Enum(
        enum_class,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=length,
        validate_strings=True,
        values_callable=lambda members: [member.value for member in members],
    )


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("webhook_attempts >= 0", name="webhook_attempts_non_negative"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_string_enum(Currency, "currency", 3))
    description: Mapped[str] = mapped_column(String(1000))
    meta: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    status: Mapped[PaymentStatus] = mapped_column(
        _string_enum(PaymentStatus, "status", 16),
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(CHAR(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    webhook_status: Mapped[WebhookStatus] = mapped_column(
        _string_enum(WebhookStatus, "webhook_status", 16),
        default=WebhookStatus.PENDING,
        server_default=WebhookStatus.PENDING.value,
    )
    webhook_attempts: Mapped[int] = mapped_column(
        Integer, default=0, server_default=text("0")
    )
    webhook_last_error: Mapped[str | None] = mapped_column(Text)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
