from enum import StrEnum


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WebhookStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


class AggregateType(StrEnum):
    PAYMENT = "payment"


class EventType(StrEnum):
    PAYMENT_CREATED = "payment.created"
