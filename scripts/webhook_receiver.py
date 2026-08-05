"""Демо-приёмник webhook'ов: печатает входящие POST'ы в stdout JSON-строками.

Не часть сервиса, утилита для проверки: контейнер webhook-sink в docker-compose.
Заодно показывает эталонную проверку подписи на стороне получателя.
Стандартная библиотека, без зависимостей. Запуск: python scripts/webhook_receiver.py [port]
"""

import hashlib
import hmac
import json
import os
import sys
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

SECRET = os.getenv("WEBHOOK_SIGNING_SECRET", "")
# Метка времени старше этого окна считается попыткой переиграть перехваченный запрос
TOLERANCE_SECONDS = 300


def verify(raw: bytes, header: str | None) -> str:
    """Проверить заголовок вида ``t=<unix>,v1=<hmac_sha256>``."""
    if not SECRET:
        return "skipped: no secret configured"
    if not header:
        return "invalid: signature header missing"
    parts = dict(part.split("=", 1) for part in header.split(",") if "=" in part)
    timestamp, signature = parts.get("t"), parts.get("v1")
    if not timestamp or not signature:
        return "invalid: malformed signature header"
    if abs(time.time() - int(timestamp)) > TOLERANCE_SECONDS:
        return "invalid: timestamp outside tolerance"
    expected = hmac.new(
        SECRET.encode(), f"{timestamp}.".encode() + raw, hashlib.sha256
    ).hexdigest()
    return "valid" if hmac.compare_digest(expected, signature) else "invalid: signature mismatch"


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        signature = verify(raw, self.headers.get("X-Webhook-Signature"))
        try:
            body: object = json.loads(raw)
        except json.JSONDecodeError:
            body = raw.decode(errors="replace")
        print(
            json.dumps(
                {
                    "ts": datetime.now(tz=UTC).isoformat(timespec="milliseconds"),
                    "event": "webhook_received",
                    "path": self.path,
                    "signature": signature,
                    "body": body,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        # Отвечаем 2xx быстро и до бизнес-логики, так требует контракт отправителя
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, format: str, *args: object) -> None:
        """Глушим стандартный access-лог."""


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    print(json.dumps({"event": "webhook_sink_started", "port": port}), flush=True)
    HTTPServer(("0.0.0.0", port), WebhookHandler).serve_forever()  # noqa: S104


if __name__ == "__main__":
    main()
