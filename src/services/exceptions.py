class ServiceError(Exception):
    pass


class IdempotencyKeyReusedError(ServiceError):
    pass


class PaymentNotFoundError(ServiceError):
    pass


class WebhookDeliveryError(ServiceError):
    pass


class WebhookTransientError(WebhookDeliveryError):
    pass


class WebhookPermanentError(WebhookDeliveryError):
    pass
