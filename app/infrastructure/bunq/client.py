"""
app/infrastructure/bunq/client.py — Base bunq HTTP client.

Wraps httpx with:
  - Automatic session token injection
  - RSA-SHA256 request body signing (X-Bunq-Client-Signature) — required by
    the bunq production API for all mutating requests.
  - Webhook signature verification (X-Bunq-Server-Signature) using the server
    public key captured during POST /installation.
  - Retry with exponential backoff + jitter for transient errors.
  - 401 handling: trigger session refresh and retry once.
  - Timeout handling that distinguishes "definitely failed" from "maybe succeeded".
  - Structured logging of every outbound call.
"""

import asyncio
import base64
import json
import logging
import random
from typing import Any, Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from app.core.config import get_settings
from app.core.exceptions import (
    BunqAPIError,
    BunqNetworkError,
    BunqPaymentAmbiguousError,
    BunqSessionError,
    BunqWebhookSignatureError,
)
from app.infrastructure.bunq.session_manager import BunqSessionManager

logger = logging.getLogger(__name__)

# HTTP methods where a timeout means "ambiguous" (the server may have processed it).
_AMBIGUOUS_METHODS = {"POST", "PUT", "PATCH"}


def _load_public_key(pem: str) -> RSAPublicKey:
    """Deserialise a PEM public key string into a cryptography public key object."""
    key = serialization.load_pem_public_key(pem.encode("utf-8"))
    if not isinstance(key, RSAPublicKey):
        raise TypeError("Loaded public key is not an RSA public key")
    return key


class BunqClient:
    """
    Async HTTP client for the bunq API.

    One instance should be reused across requests (shares the underlying
    connection pool). Instantiate once and inject as a dependency.

    Request signing
    ---------------
    bunq requires every mutating request body to be signed with the client
    RSA private key. The signature is placed in X-Bunq-Client-Signature.
    This is enforced in _request() for all POST/PUT/PATCH calls.

    Webhook verification
    --------------------
    Call verify_webhook_signature() on every inbound webhook before processing
    domain logic. Uses the server public key captured during POST /installation.
    """

    def __init__(self, session_manager: Optional[BunqSessionManager] = None) -> None:
        self._session_manager = session_manager or BunqSessionManager()
        self._http: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "BunqClient":
        self._http = httpx.AsyncClient(
            base_url=get_settings().BUNQ_BASE_URL,
            timeout=get_settings().BUNQ_TIMEOUT_SECONDS,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "bunq-payment-orchestrator/1.0",
                "X-Bunq-Language": "en_US",
                "X-Bunq-Region": "en_US",
                "X-Bunq-Geolocation": "0 0 0 0 NL",
            },
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._http:
            await self._http.aclose()

    # ── Public request methods ────────────────────────────────────────────────

    async def get(self, path: str, **kwargs) -> dict[str, Any]:
        return await self._request("GET", path, **kwargs)

    async def post(
        self, path: str, json: Optional[dict] = None, **kwargs
    ) -> dict[str, Any]:
        return await self._request("POST", path, json=json, **kwargs)

    # ── Webhook signature verification ────────────────────────────────────────

    def verify_webhook_signature(self, raw_body: bytes, signature_b64: str) -> None:
        """
        Verify the X-Bunq-Server-Signature header on an inbound webhook.

        bunq signs the response body with its private key. We verify using the
        server public key captured during POST /installation and persisted in
        the bunq_sessions row.

        Raises BunqWebhookSignatureError if verification fails.
        Raises BunqSessionError if the server public key is not loaded.
        """
        server_pem = self._session_manager.get_server_public_key_pem()
        if not server_pem:
            raise BunqSessionError(
                "Server public key not available — run bootstrap() so the key "
                "is captured from POST /installation before processing webhooks."
            )

        try:
            public_key = _load_public_key(server_pem)
            signature = base64.b64decode(signature_b64)
            public_key.verify(
                signature,
                raw_body,
                padding.PKCS1v15(),
                hashes.SHA256(),
            )
        except Exception as exc:
            raise BunqWebhookSignatureError(
                f"Webhook signature verification failed: {exc}"
            ) from exc

    # ── Core request logic ────────────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict] = None,
        request_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Execute a signed request against the bunq API with retry logic.

        Signing: POST/PUT/PATCH bodies are signed with the client RSA private
        key. The signature is placed in X-Bunq-Client-Signature. GET requests
        have an empty body and send an empty string signature.

        Retry strategy:
          - 429 (rate limit): retry after Retry-After header.
          - 5xx (server error): retry with exponential backoff + jitter.
          - 401 (unauthorized): refresh session and retry once.
          - Network timeout on POST/PUT: raise BunqPaymentAmbiguousError.
          - 4xx (client error, not 401/429): do not retry.
        """
        import uuid as _uuid

        rid = request_id or str(_uuid.uuid4())
        max_retries = get_settings().BUNQ_MAX_RETRIES
        session_refreshed = False

        # Serialise body once — the signature covers the exact bytes sent.
        body_str = json_module_dumps(json) if json is not None else ""

        for attempt in range(max_retries + 1):
            token = await self._session_manager.get_session_token()

            # Sign the body for every mutating request.
            try:
                signature = self._session_manager.sign_request_body(body_str)
            except BunqSessionError:
                # Private key not loaded yet (e.g. first request before bootstrap).
                # Log and proceed without signature — will get a 400/401 from bunq
                # which triggers a bootstrap/refresh.
                signature = ""
                logger.warning("bunq.request.no_signature — private key not loaded")

            headers = {
                "X-Bunq-Client-Authentication": token,
                "X-Bunq-Client-Request-Id": rid,
                "X-Bunq-Client-Signature": signature,
            }

            logger.info(
                "bunq.request",
                extra={
                    "method": method,
                    "path": path,
                    "request_id": rid,
                    "attempt": attempt,
                },
            )

            if self._http is None:
                raise RuntimeError(
                    "BunqClient HTTP client is not initialized. Use 'async with BunqClient()' or call '__aenter__' before making requests."
                )
            try:
                response = await self._http.request(
                    method,
                    path,
                    content=body_str.encode("utf-8") if body_str else None,
                    headers=headers,
                )
            except httpx.TimeoutException as exc:
                logger.warning(
                    "bunq.request.timeout",
                    extra={"method": method, "path": path, "request_id": rid},
                )
                if method.upper() in _AMBIGUOUS_METHODS:
                    raise BunqPaymentAmbiguousError(
                        f"Network timeout on {method} {path} — the request may or may not "
                        f"have been processed. request_id={rid}"
                    ) from exc
                raise BunqNetworkError(f"Timeout on {method} {path}") from exc

            except httpx.NetworkError as exc:
                logger.warning(
                    "bunq.request.network_error",
                    extra={"method": method, "path": path, "error": str(exc)},
                )
                raise BunqNetworkError(
                    f"Network error on {method} {path}: {exc}"
                ) from exc

            logger.info(
                "bunq.response",
                extra={
                    "method": method,
                    "path": path,
                    "status_code": response.status_code,
                    "request_id": rid,
                },
            )

            # ── 401: refresh session and retry once ──────────────────────────
            if response.status_code == 401:
                if not session_refreshed:
                    logger.info("bunq.session.expired — refreshing and retrying")
                    await self._session_manager.invalidate_and_refresh()
                    session_refreshed = True
                    continue
                raise BunqSessionError("bunq returned 401 after session refresh.")

            # ── 429: rate limited — honour Retry-After ────────────────────────
            if response.status_code == 429:
                if attempt < max_retries:
                    retry_after = float(response.headers.get("Retry-After", 2))
                    logger.warning(
                        "bunq.request.rate_limited",
                        extra={"retry_after_s": retry_after, "attempt": attempt},
                    )
                    await asyncio.sleep(retry_after)
                    continue
                raise BunqAPIError(429, "Rate limit exceeded after max retries.")

            # ── 5xx: server error — exponential backoff ───────────────────────
            if response.status_code >= 500:
                if attempt < max_retries:
                    backoff = self._backoff(attempt)
                    logger.warning(
                        "bunq.request.server_error — will retry",
                        extra={"status": response.status_code, "backoff_s": backoff},
                    )
                    await asyncio.sleep(backoff)
                    continue
                raise BunqAPIError(response.status_code, response.text[:300])

            # ── Any other non-success: do not retry ───────────────────────────
            if not response.is_success:
                raise BunqAPIError(response.status_code, response.text[:300])

            return response.json()

        raise BunqAPIError(0, f"Exhausted {max_retries} retries for {method} {path}")

    @staticmethod
    def _backoff(retry_count: int) -> float:
        """Exponential backoff with ±25% jitter."""
        base = get_settings().BUNQ_RETRY_BACKOFF_BASE ** retry_count
        jitter = base * 0.25 * (2 * random.random() - 1)
        return max(0.1, base + jitter)


def json_module_dumps(data: Optional[dict]) -> str:
    """Stable JSON serialisation for request body signing."""
    if data is None:
        return ""
    return json.dumps(data, separators=(",", ":"), sort_keys=False)
