class ServiceError(Exception):
    pass


class IdempotencyKeyReusedError(ServiceError):
    pass


class PaymentNotFoundError(ServiceError):
    pass
