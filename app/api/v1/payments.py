"""HTTP-слой платежей: тонкие хендлеры, вся логика в PaymentService."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.deps import get_payment_service
from app.schemas import ErrorResponse, PaymentCreate, PaymentCreated, PaymentOut
from app.services.payments import PaymentService

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Уникальный ключ запроса для защиты от дублей (обязательный)",
    ),
]
Service = Annotated[PaymentService, Depends(get_payment_service)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreated,
    summary="Создать платёж",
    description=(
        "Принимает платёж в обработку; результат придёт на webhook_url. Повтор с тем же "
        "Idempotency-Key и телом возвращает исходный платёж без дублирования."
    ),
    responses={
        401: {"model": ErrorResponse, "description": "Неверный или отсутствующий X-API-Key"},
        409: {
            "model": ErrorResponse,
            "description": "Idempotency-Key уже использован с другим телом запроса",
        },
    },
)
async def create_payment(
    body: PaymentCreate, idempotency_key: IdempotencyKey, service: Service
) -> PaymentCreated:
    payment, _created = await service.create_payment(body, idempotency_key)
    return PaymentCreated(
        payment_id=payment.payment_id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentOut,
    summary="Получить платёж",
    responses={
        401: {"model": ErrorResponse, "description": "Неверный или отсутствующий X-API-Key"},
        404: {"model": ErrorResponse, "description": "Платёж не найден"},
    },
)
async def get_payment(payment_id: uuid.UUID, service: Service) -> PaymentOut:
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return payment
