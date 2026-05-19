"""
app/api/middleware/auth.py — Pure-ASGI API key authentication middleware.

Replaces BaseHTTPMiddleware with a raw ASGI callable. Validates the
X-API-Key header using secrets.compare_digest to prevent timing attacks.

Public paths (health, docs, bunq webhooks) bypass authentication.
"""

from __future__ import annotations

import json
import logging
import secrets

from starlette.types import ASGIApp, Receive, Scope, Send

from app.core.config import get_settings

logger = logging.getLogger(__name__)

PUBLIC_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}
_BUNQ_WEBHOOK_PREFIX = "/webhooks/bunq"

_401_BODY = json.dumps(
    {"error": "unauthorized", "detail": "Invalid or missing API key."}
).encode()
_401_HEADERS = [
    (b"content-type", b"application/json"),
    (b"content-length", str(len(_401_BODY)).encode()),
]


def assert_api_key_configured() -> None:
    """
    Fail fast at startup if API_KEY is not set in non-debug mode.
    Called once from the lifespan context manager.
    """
    settings = get_settings()
    if not settings.DEBUG and not settings.API_KEY:
        raise RuntimeError(
            "API_KEY must be set in production (DEBUG=false). "
            "Set API_KEY in your environment or .env file."
        )
    if not settings.API_KEY:
        logger.warning(
            "API_KEY is not set — all requests are allowed. "
            "Acceptable only in DEBUG mode."
        )


class APIKeyMiddleware:
    """Pure-ASGI API key middleware. No BaseHTTPMiddleware dependency."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")

        if path in PUBLIC_PATHS or path.startswith(_BUNQ_WEBHOOK_PREFIX):
            await self.app(scope, receive, send)
            return

        expected = get_settings().API_KEY
        if not expected:
            # No key configured — allow all (dev/sandbox mode).
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        provided = headers.get(b"x-api-key", b"").decode()

        if not secrets.compare_digest(provided.encode(), expected.encode()):
            client = scope.get("client")
            logger.warning(
                "auth.rejected",
                extra={
                    "http.path": path,
                    "client.ip": client[0] if client else None,
                },
            )
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": _401_HEADERS,
                }
            )
            await send({"type": "http.response.body", "body": _401_BODY})
            return

        await self.app(scope, receive, send)
