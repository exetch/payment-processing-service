"""FastAPI-зависимости: аутентификация и доступ к сервисам."""

from typing import Annotated

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import get_settings
from app.core.security import api_key_is_valid
from app.services.payments import PaymentService

_api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="Статический API-ключ сервиса",
)


async def require_api_key(api_key: Annotated[str | None, Security(_api_key_header)]) -> None:
    if api_key is None or not api_key_is_valid(api_key, get_settings().api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


def get_payment_service(request: Request) -> PaymentService:
    service: PaymentService = request.app.state.payment_service
    return service
