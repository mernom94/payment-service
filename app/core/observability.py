"""
app/core/observability.py — Prometheus metrics and OTLP tracing.

Metrics
-------
All counters and histograms are created once at module import time and
exported via /metrics (mounted in main.py via prometheus_client's
make_asgi_app).

  payments_submitted_total        — Counter, labels: currency, state_transition
  webhook_events_processed_total  — Counter, labels: event_type, outcome
  dlq_entries_total               — Counter, labels: worker
  payment_latency_seconds         — Histogram, labels: stage
  ledger_balance_drift            — Gauge,   labels: account_id

Tracing (OTLP)
--------------
Initialised once via init_tracing(). Spans are created with
get_tracer(__name__) in each module. The OTLP exporter is configured from
OTLP_ENDPOINT (defaults to localhost:4317 for local Jaeger/Tempo).

If prometheus_client or opentelemetry packages are not installed the module
degrades gracefully: metrics are no-ops and tracing is a no-op tracer.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Gauge, Histogram

    payments_submitted_total = Counter(
        "payments_submitted_total",
        "Total payment submissions by currency and outcome",
        ["currency", "state_transition"],
    )

    webhook_events_processed_total = Counter(
        "webhook_events_processed_total",
        "Total webhook events processed by event_type and outcome",
        ["event_type", "outcome"],
    )

    dlq_entries_total = Counter(
        "dlq_entries_total",
        "Total dead-letter queue entries created",
        ["worker"],
    )

    payment_latency_seconds = Histogram(
        "payment_latency_seconds",
        "End-to-end payment processing latency by stage",
        ["stage"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    )

    ledger_balance_drift = Gauge(
        "ledger_balance_drift",
        "Absolute balance drift detected during reconciliation (in currency units)",
        ["account_id"],
    )

    _PROMETHEUS_AVAILABLE = True
    logger.debug("Prometheus metrics initialised")

except ImportError:
    logger.warning(
        "prometheus_client not installed — metrics disabled. "
        "Add prometheus-client to requirements.txt."
    )
    _PROMETHEUS_AVAILABLE = False

    # No-op fallbacks so call sites don't need try/except.
    class _NoOp:
        def labels(self, **_):
            return self

        def inc(self, *_, **__):
            pass

        def observe(self, *_, **__):
            pass

        def set(self, *_, **__):
            pass

    payments_submitted_total = _NoOp()
    webhook_events_processed_total = _NoOp()
    dlq_entries_total = _NoOp()
    payment_latency_seconds = _NoOp()
    ledger_balance_drift = _NoOp()


# ---------------------------------------------------------------------------
# OTLP tracing
# ---------------------------------------------------------------------------

_tracer_provider = None


def init_tracing(
    service_name: str = "payment-service",
    otlp_endpoint: Optional[str] = None,
) -> None:
    """
    Initialise OpenTelemetry tracing with an OTLP gRPC exporter.

    Call once from the lifespan context manager. Idempotent: a second call
    is a no-op.
    """
    global _tracer_provider
    if _tracer_provider is not None:
        return

    endpoint = otlp_endpoint or os.getenv("OTLP_ENDPOINT", "http://localhost:4317")

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _tracer_provider = provider
        logger.info(
            "OTLP tracing initialised",
            extra={"service_name": service_name, "endpoint": endpoint},
        )
    except ImportError:
        logger.warning(
            "opentelemetry packages not installed — tracing disabled. "
            "Add opentelemetry-sdk opentelemetry-exporter-otlp to requirements.txt."
        )


def get_tracer(name: str):
    """
    Return a tracer for the given module name.

    Returns a no-op tracer when OpenTelemetry is not installed.
    """
    try:
        from opentelemetry import trace

        return trace.get_tracer(name)
    except ImportError:
        return _NoOpTracer()


class _NoOpTracer:
    """Minimal no-op tracer used when opentelemetry is not installed."""

    def start_as_current_span(self, name, **_):
        from contextlib import contextmanager

        @contextmanager
        def _noop():
            yield _NoOpSpan()

        return _noop()


class _NoOpSpan:
    def set_attribute(self, *_, **__):
        pass

    def set_status(self, *_, **__):
        pass

    def record_exception(self, *_, **__):
        pass
