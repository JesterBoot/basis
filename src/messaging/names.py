from enum import StrEnum


class Exchange(StrEnum):
    PAYMENTS = "payments"
    PAYMENTS_DLX = "payments.dlx"


class Queue(StrEnum):
    PAYMENTS_NEW = "payments.new"
    PAYMENTS_DLQ = "payments.new.dlq"


class RoutingKey(StrEnum):
    PAYMENTS_NEW = "payments.new"
    PAYMENTS_DLQ = "payments.new.dlq"


class Header(StrEnum):
    ATTEMPT = "x-attempt"


def retry_queue_name(delay_ms: int) -> str:
    return f"{Queue.PAYMENTS_NEW}.retry.{delay_ms}ms"
