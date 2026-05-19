"""
app/api/middleware/correlation.py — Pure-ASGI correlation ID middleware.

Replaces BaseHTTPMiddleware with a raw ASGI callable to avoid the known
double-buffering and exception-masking issues in Starlette's
BaseHTTPMiddleware.

Every inbound request receives a correlation_id. If the caller supplies one
via X-Correlation-ID we honour it; otherwise we mint a fresh UUID4. The ID is:
  - Stored in request.state for downstream handlers.
  - Bound into the structlog context for the duration of the request.
  - Written into correlation_id_var so the stdlib logging handler picks it up.
  - Echoed in the X-Correlation-ID response header.
"""

from __future__ import annotations

import uuid

import structlog
from starlette.types import ASGIApp, Receive, Scope, Send

from app.core.logging import correlation_id_var

HEADER_NAME = b"x-correlation-id"
HEADER_NAME_STR = "X-Correlation-ID"


class CorrelationIDMiddleware:
    """Pure-ASGI correlation ID middleware. No BaseHTTPMiddleware dependency."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        # Extract correlation ID from request headers (already lower-cased in ASGI).
        headers = dict(scope.get("headers", []))
        raw = headers.get(HEADER_NAME)
        correlation_id = raw.decode() if raw else str(uuid.uuid4())

        # Expose on scope["state"] so Starlette's Request.state can read it.
        scope.setdefault("state", {})
        scope["state"]["correlation_id"] = correlation_id

        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
        token = correlation_id_var.set(correlation_id)

        async def send_with_header(message):
            if message["type"] == "http.response.start":
                # Append correlation ID to response headers.
                headers_out = list(message.get("headers", []))
                headers_out.append((HEADER_NAME, correlation_id.encode()))
                message = {**message, "headers": headers_out}
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            correlation_id_var.reset(token)
            structlog.contextvars.unbind_contextvars("correlation_id")
