"""Точка входа API-процесса: FastAPI + фоновый outbox relay."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import Depends, FastAPI, Request, status
from fastapi.responses import JSONResponse
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.deps import require_api_key
from app.api.v1.payments import router as payments_router
from app.core.broker import build_broker
from app.core.config import get_settings
from app.core.db import build_engine, build_session_factory
from app.core.exceptions import IdempotencyConflictError
from app.core.logging import setup_logging
from app.core.net import UnsafeWebhookTargetError
from app.repositories.uow import UnitOfWork
from app.services.payments import PaymentService
from app.workers.outbox_relay import OutboxRelay
from app.workers.topology import declare_topology

logger = logging.getLogger(__name__)

RELAY_FAILURE_THRESHOLD = 3
BROKER_PING_TIMEOUT_SECONDS = 3.0


def _report_relay_exit(task: asyncio.Task[None]) -> None:
    """Relay крутится, пока жив процесс: любой выход, кроме отмены, это авария.

    Без этого исключение осело бы в «task exception never retrieved», а сервис
    продолжил бы принимать платежи, не публикуя события.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.critical("outbox relay died", exc_info=exc)
    else:
        logger.critical("outbox relay exited unexpectedly")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    engine = build_engine()
    session_factory = build_session_factory(engine)
    app.state.engine = engine
    app.state.payment_service = PaymentService(
        lambda: UnitOfWork(session_factory),
        allowed_hosts=settings.webhook_allowed_host_set,
    )

    broker = build_broker(settings.rabbitmq_url)
    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)
    app.state.broker = broker
    app.state.relay = relay
    relay_task: asyncio.Task[None] | None = None
    try:
        await broker.connect()
        await declare_topology(broker, settings)
        relay_task = asyncio.create_task(relay.run(), name="outbox-relay")
        relay_task.add_done_callback(_report_relay_exit)
        app.state.relay_task = relay_task
        logger.info("api started")
        yield
    finally:
        if relay_task is not None:
            relay_task.cancel()
            with suppress(asyncio.CancelledError):
                await relay_task
        await broker.close()
        await engine.dispose()
        logger.info("api stopped")


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings.log_level)

    app = FastAPI(
        title="Payment Processing Service",
        description="Асинхронный сервис процессинга платежей",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(payments_router, dependencies=[Depends(require_api_key)])

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict_handler(
        _: Request, exc: IdempotencyConflictError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)}
        )

    @app.exception_handler(UnsafeWebhookTargetError)
    async def unsafe_webhook_handler(_: Request, exc: UnsafeWebhookTargetError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": f"webhook_url is not allowed: {exc}"},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        """Единый формат ошибок и структурный лог даже для того, чего мы не ждали.

        Без него Starlette отдаёт text/plain «Internal Server Error», выпадая из
        контракта остальных ответов. Наружу уходит только общая формулировка:
        текст исключения может содержать параметры запроса и куски SQL.
        """
        logger.exception(
            "unhandled error",
            extra={"ctx": {"method": request.method, "path": request.url.path}},
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        """Liveness: процесс жив и обслуживает HTTP."""
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> JSONResponse:
        """Readiness: доступны БД и брокер, работает outbox relay."""
        checks = {
            "database": await _check_database(request.app.state.engine),
            "broker": await _check_broker(request.app.state.broker),
            "outbox_relay": _check_relay(
                request.app.state.relay_task, request.app.state.relay
            ),
        }
        ready = all(result == "ok" for result in checks.values())
        if not ready:
            logger.warning("readiness check failed", extra={"ctx": {"checks": checks}})
        return JSONResponse(
            status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "ready" if ready else "not ready", "checks": checks},
        )

    return app


async def _check_database(engine: AsyncEngine) -> str:
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as exc:
        return f"error: {type(exc).__name__}"
    return "ok"


async def _check_broker(broker: RabbitBroker) -> str:
    try:
        alive = await broker.ping(BROKER_PING_TIMEOUT_SECONDS)
    except Exception as exc:
        return f"error: {type(exc).__name__}"
    return "ok" if alive else "unreachable"


def _check_relay(task: asyncio.Task[None], relay: OutboxRelay) -> str:
    if task.done():
        return "dead"
    if relay.consecutive_failures >= RELAY_FAILURE_THRESHOLD:
        return f"failing: {relay.consecutive_failures} iterations in a row"
    return "ok"


app = create_app()
