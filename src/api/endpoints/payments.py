import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from core.config import get_settings
from schemas.payment import PaymentCreate, PaymentCreated, PaymentRead
from services.exceptions import IdempotencyKeyReusedError, PaymentNotFoundError
from services.payments import PaymentService, get_payment_service

router = APIRouter(prefix="/payments", tags=["Payments"])

PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]

_settings = get_settings()

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=_settings.idempotency_key_max_length,
        pattern=_settings.idempotency_key_pattern,
    ),
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreated,
    summary="Create a payment",
    responses={
        status.HTTP_409_CONFLICT: {
            "description": "Idempotency-Key was already used with a different body"
        },
    },
)
async def create_payment_endpoint(
    payload: PaymentCreate,
    idempotency_key: IdempotencyKey,
    service: PaymentServiceDep,
    response: Response,
) -> PaymentCreated:
    try:
        result = await service.create(payload, idempotency_key)
    except IdempotencyKeyReusedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key already used with a different request body",
        ) from exc

    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return result.payment


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Get payment details",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Payment not found"}},
)
async def get_payment_endpoint(
    payment_id: uuid.UUID, service: PaymentServiceDep
) -> PaymentRead:
    try:
        return await service.get(payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        ) from exc
