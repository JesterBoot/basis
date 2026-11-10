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
