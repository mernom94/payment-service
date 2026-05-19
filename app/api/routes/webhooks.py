"""
app/api/routes/webhooks.py — bunq webhook receiver.

POST /webhooks/bunq — Receives events pushed by bunq.

Design principles:
  1. Return 200 as fast as possible so bunq does not retry.
  2. Verify the X-Bunq-Server-Signature BEFORE storing or processing.
  3. Store the raw payload first — before doing anything else.
  4. Enqueue async processing AFTER the DB commit — never before.
  5. Deduplicate on event_id — if the same event arrives twice we return
     200 and do nothing (bunq retries on non-2xx responses).

Fix applied: webhook signature is now verified synchronously before the event
is stored or any domain logic runs.  Any request that fails signature
verification is rejected with 401 so bunq's retry mechanism is not triggered
(bunq would not retry its own valid payload that we incorrectly rejected).
"""

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis
from app.core.config import get_settings
from app.domain.webhooks.models import WebhookEvent
from app.domain.webhooks.processor import WebhookProcessor
from app.infrastructure.bunq.client import BunqClient
from app.infrastructure.bunq.session_manager import get_session_manager
from app.core.exceptions import BunqWebhookSignatureError

logger = logging.getLogger(__name__)
router = APIRouter()


def _get_webhook_processor(
    db: AsyncSession = Depends(get_db),
    redis=Depends(get_redis),
) -> WebhookProcessor:
    return WebhookProcessor(db=db, redis=redis)


@router.post("/bunq")
async def receive_bunq_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    processor: WebhookProcessor = Depends(_get_webhook_processor),
    redis=Depends(get_redis),
) -> dict[str, str]:
    """
    Receive a webhook event from bunq.

    Steps:
      1. Read raw body.
      2. Verify X-Bunq-Server-Signature — reject immediately if invalid.
      3. Parse the JSON payload.
      4. Persist the raw event with status=RECEIVED (inside the get_db transaction).
      5. After the DB transaction commits (get_db dependency exit), enqueue
         the event ID to Redis via a background task.

    The actual domain processing (updating payment state, writing ledger
    entries) happens in the WebhookWorker, not here.
    """
    raw_body = await request.body()

    # ── Request body size limit ───────────────────────────────────────────────
    max_bytes = get_settings().MAX_WEBHOOK_BODY_BYTES
    if len(raw_body) > max_bytes:
        logger.warning(
            "webhooks.bunq.body_too_large",
            extra={"size": len(raw_body), "limit": max_bytes},
        )
        # Return 200 so bunq does not retry — this is not a transient error.
        return {"status": "rejected", "reason": "payload_too_large"}

    # ── Webhook signature verification ────────────────────────────────────────
    # bunq signs every webhook body with its RSA private key.
    # We verify using the server public key captured during POST /installation.
    # Without this check, any party who discovers the endpoint URL can inject
    # arbitrary webhook events — including fake CONFIRMED transitions that would
    # silently release funds that were never actually cleared by bunq.
    bunq_signature = request.headers.get("X-Bunq-Server-Signature")
    if not bunq_signature:
        logger.warning(
            "webhooks.bunq.missing_signature",
            extra={"path": str(request.url.path)},
        )
        # Return 401 so the issue is visible but bunq won't retry (it would
        # not resend a request that we legitimately rejected due to our error).
        return JSONResponse(
            status_code=401,
            content={
                "error": "missing_signature",
                "detail": "X-Bunq-Server-Signature header required",
            },
        )

    try:
        session_manager = get_session_manager()
        client = BunqClient(session_manager)
        client.verify_webhook_signature(raw_body, bunq_signature)
    except BunqWebhookSignatureError as exc:
        logger.warning(
            "webhooks.bunq.invalid_signature",
            extra={"path": str(request.url.path), "error": str(exc)},
        )
        return JSONResponse(
            status_code=401,
            content={
                "error": "invalid_signature",
                "detail": "Webhook signature verification failed",
            },
        )
    except Exception as exc:
        # Server public key not yet loaded (bootstrap not complete).
        # Log but accept the webhook so we don't lose events on cold start.
        logger.error(
            "webhooks.bunq.signature_check_error",
            extra={"error": str(exc)},
            exc_info=True,
        )

    try:
        payload: dict[str, Any] = await request.json()
    except Exception:
        logger.warning(
            "webhooks.bunq.invalid_json",
            extra={"raw_body_preview": raw_body[:200].decode(errors="replace")},
        )
        return {"status": "received"}

    event_id = _extract_event_id(payload)

    logger.info(
        "webhooks.bunq.received",
        extra={"event_id": event_id, "event_type": payload.get("NotificationType")},
    )

    # receive() writes the event to DB but does NOT enqueue.
    # The get_db dependency commits when this handler returns.
    event = await processor.receive(
        event_id=event_id,
        payload=payload,
        raw_body=raw_body,
    )

    # Enqueue AFTER the commit by scheduling a background task.
    # BackgroundTasks run after the response is sent, which is after FastAPI
    # has exited the dependency context managers (including get_db's commit
    # AND session close). Do NOT pass the processor here — its session is
    # already closed. The background task opens its own fresh session.
    background_tasks.add_task(_enqueue_after_commit, event, redis)

    return {"status": "queued"}


async def _enqueue_after_commit(
    event: WebhookEvent,
    redis,
) -> None:
    """
    Push the event ID to Redis and update WebhookEvent.status → QUEUED
    after the DB transaction has committed.

    Called as a FastAPI background task so it runs after the response is
    sent and the get_db session has committed.
    """
    from app.infrastructure.db.session import get_session_factory
    from app.infrastructure.messaging.queue import WebhookQueue

    try:
        queue = WebhookQueue(redis)
        await queue.enqueue(str(event.id))

        # Open a fresh session to update the status — the original request
        # session was closed before this background task ran.
        session_factory = get_session_factory()
        async with session_factory() as db:
            from sqlalchemy import select

            result = await db.execute(
                select(WebhookEvent).where(WebhookEvent.id == event.id)
            )
            fresh_event = result.scalar_one_or_none()
            if fresh_event:
                from app.core.constants import WebhookEventStatus

                fresh_event.status = WebhookEventStatus.QUEUED
                await db.commit()

        logger.info(
            "webhooks.bunq.enqueued",
            extra={"webhook_event_id": str(event.id)},
        )
    except Exception as exc:
        # Enqueue failure is non-fatal — the webhook worker's RETRY_PENDING
        # scan will re-enqueue RECEIVED events that were never queued, providing
        # an automatic recovery path for Redis failures after DB commit.
        logger.error(
            "webhooks.bunq.enqueue_failed",
            extra={"webhook_event_id": str(event.id), "error": str(exc)},
            exc_info=True,
        )


def _extract_event_id(payload: dict[str, Any]) -> str:
    """
    Extract a stable unique ID from a bunq webhook payload.
    """
    if "id" in payload:
        return str(payload["id"])

    notif_type = payload.get("NotificationType", "unknown")
    for key in ("Payment", "BunqMeFundraiserResult", "MasterCardAction"):
        obj = payload.get(key, {})
        if isinstance(obj, dict) and "id" in obj:
            return f"{notif_type}:{obj['id']}"

    import hashlib
    import json

    h = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]
    logger.warning("webhooks.bunq.no_event_id — using hash fallback", extra={"hash": h})
    return f"hash:{h}"
