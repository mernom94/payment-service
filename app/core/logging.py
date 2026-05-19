"""
app/core/logging.py — Structured logging configuration.

Configures Python's standard logging to emit JSON lines in production
(easy to ingest into Datadog / CloudWatch) and human-readable text in
development. Every log record is enriched with a correlation_id when one
is present in the current context.
"""

import logging
import sys
from contextvars import ContextVar
from typing import Optional

import structlog

from app.core.config import get_settings

# Context variable holding the current request's correlation ID.
# Set by CorrelationIDMiddleware; read by the structlog processor below.
correlation_id_var: ContextVar[Optional[str]] = ContextVar(
    "correlation_id", default=None
)


def _add_correlation_id(
    logger: logging.Logger,  # noqa: ARG001
    method_name: str,  # noqa: ARG001
    event_dict: dict,
) -> dict:
    """structlog processor that injects correlation_id from context."""
    cid = correlation_id_var.get()
    if cid:
        event_dict["correlation_id"] = cid
    return event_dict


def configure_logging() -> None:
    """
    Call once at application startup (before any log statements).

    - JSON renderer for production / structured log sinks.
    - ConsoleRenderer for local development (LOG_FORMAT=text).
    - Standard-library logging is redirected through structlog so third-party
      libraries (SQLAlchemy, httpx, etc.) are captured in the same format.
    """
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        _add_correlation_id,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if get_settings().LOG_FORMAT == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(get_settings().LOG_LEVEL)

    # Silence noisy third-party loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
