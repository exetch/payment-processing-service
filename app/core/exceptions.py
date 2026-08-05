"""Доменные исключения."""


class DomainError(Exception):
    """Базовая доменная ошибка."""


class IdempotencyConflictError(DomainError):
    """Idempotency-Key уже использован с другими параметрами платежа."""

    def __init__(self, key: str) -> None:
        super().__init__(
            f"Idempotency-Key {key!r} уже использован с другими параметрами платежа"
        )
        self.key = key


class DuplicateIdempotencyKeyError(DomainError):
    """Нарушение UNIQUE(idempotency_key): гонка двух одновременных POST с одним ключом."""
