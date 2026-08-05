"""Имена объектов RabbitMQ-топологии."""

EXCHANGE_PAYMENTS = "payments"
EXCHANGE_RETRY = "payments.retry"
EXCHANGE_DLX = "payments.dlx"

QUEUE_PAYMENTS_NEW = "payments.new"
QUEUE_PAYMENTS_DLQ = "payments.new.dlq"

ROUTING_KEY_PAYMENTS_NEW = "payments.new"

HEADER_ATTEMPT = "x-attempt"
HEADER_LAST_ERROR = "x-last-error"
HEADER_DLQ_REASON = "x-dlq-reason"


def retry_queue_name(tier: int) -> str:
    """Имя retry-очереди тира ``tier`` (нумерация с 1)."""
    return f"payments.new.retry.{tier}"
