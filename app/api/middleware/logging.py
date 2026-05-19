"""
app/api/middleware/logging.py — Pure-ASGI request / response access logging.

Replaces BaseHTTPMiddleware to avoid its double-buffering and error-masking
behaviour. Logs every HTTP request with method, path, status code, and
latency. Runs *inside* CorrelationIDMiddleware so the correlation_id context
variable is already populated.
"""

from __future__ import annotations

import logging
import time

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware:
    """Pure-ASGI request logging middleware. No BaseHTTPMiddleware dependency."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "")
        path = scope.get("path", "")
        # path_params are populated by Starlette's router after routing, so
        # they're not available here; extract payment_id via simple string split.
        parts = path.strip("/").split("/")
        payment_id = None
        if "payments" in parts:
            idx = parts.index("payments")
            if idx + 1 < len(parts):
                payment_id = parts[idx + 1]

        query = scope.get("query_string", b"").decode()
        client = scope.get("client")
        client_ip = client[0] if client else None

        logger.info(
            "request.started",
            extra={
                "http.method": method,
                "http.path": path,
                "http.query": query,
                "payment_id": payment_id,
                "client.ip": client_ip,
            },
        )

        start = time.perf_counter()
        status_code: int = 0

        async def send_capturing_status(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)
            await send(message)

        try:
            await self.app(scope, receive, send_capturing_status)
            elapsed_ms = (time.perf_counter() - start) * 1000
            level = logging.WARNING if status_code >= 400 else logging.INFO
            logger.log(
                level,
                "request.completed",
                extra={
                    "http.method": method,
                    "http.path": path,
                    "http.status_code": status_code,
                    "http.latency_ms": round(elapsed_ms, 2),
                    "payment_id": payment_id,
                },
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.error(
                "request.failed",
                extra={
                    "http.method": method,
                    "http.path": path,
                    "http.latency_ms": round(elapsed_ms, 2),
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise
