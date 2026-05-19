# Bunq Payment Orchestrator
Live demo:(https://payment-orchestrator-nu.vercel.app)

A production-grade payment orchestration backend integrating with the [bunq](https://www.bunq.com/) sandbox API. Built with financial correctness as the primary constraint — not throughput, not developer convenience.

The system implements the reliability primitives that payment infrastructure demands: an outbox pattern for guaranteed delivery, double-entry ledger accounting, idempotency at every layer, exponential-backoff retries, distributed locking, and webhook deduplication. These are not optional features — they are load-bearing correctness guarantees without which money moves incorrectly under failure.

---

## Why This Exists

Integrating directly with a payment API (POST payment → get back an ID) appears simple. It becomes dangerous the moment you consider partial failures:

- What happens if you call bunq and the network drops before you get a response? Did the payment go through?
- What if your server crashes after bunq accepts the payment but before you store the ID?
- What if a webhook confirming a payment is delivered twice?
- What if two concurrent API calls carry the same idempotency key?

Every one of these scenarios results in either a duplicate payment (money sent twice) or a lost payment (money never sent, but marked as sent). Neither is acceptable. This project addresses each failure mode explicitly, with corresponding tests.

---

## Reliability & Correctness Goals

| Goal | Mechanism |
|---|---|
| No duplicate payments | Distributed lock + DB unique constraint + idempotency cache |
| No lost payments on crash | Transactional outbox + stuck-state recovery |
| Accurate ledger under concurrent load | Double-entry bookkeeping + `SELECT FOR UPDATE` + SERIALIZABLE isolation for balance writes |
| Idempotent webhook processing | `processed_webhook_events` table + UNIQUE constraint on `event_id` |
| Recoverable ambiguous payments | Reconciliation worker cross-checks bunq balance every 5 minutes |
| Replay attack prevention | Webhook RSA signature verification before any domain logic runs |

---

## Features

### Payment Orchestration
- Accepts payment creation requests via REST API and immediately returns a response
- Defers the actual bunq API call to a background worker via the outbox pattern
- Tracks payment lifecycle through a strict state machine: `PENDING → PROCESSING → SUBMITTED → CONFIRMED` (or `FAILED`)
- Decouples API latency from external payment network latency

### Idempotency Guarantees
- Three-layer idempotency on `POST /payments`:
  1. **Redis cache** — O(1) lookup, returns cached result for known `external_id` before touching the database
  2. **Distributed lock** — prevents concurrent requests with the same `external_id` from both proceeding past the duplicate check
  3. **DB UNIQUE constraint** — last-resort guard if Redis is unavailable or the lock expires; `IntegrityError` is caught and converted to a clean `DuplicatePaymentError`
- Idempotency cache is backfilled on DB-hit so future requests hit the fast path

### Retry System with Exponential Backoff
- Failed outbox records are rescheduled with capped exponential backoff: `min(2^retry_count, 300)` seconds
- `outbox.retry_count` is the authoritative counter driving scheduling; `payment.retry_count` is a denormalised mirror for observability
- Maximum 5 retries (configurable via `MAX_PAYMENT_RETRIES`); after exhaustion the payment is permanently marked `FAILED`
- Retries re-queue via `outbox.next_attempt_at` — the worker polls `WHERE next_attempt_at IS NULL OR next_attempt_at <= now()`

### Distributed Locking
- Redis `SET NX PX` (set-if-not-exists with millisecond TTL) for single-node Redis — correct and simpler than Redlock
- Unique per-acquisition token (`uuid4`) prevents a process from releasing a lock it no longer owns after TTL expiry
- Lock release uses a Lua script for atomicity (check ownership + delete is a single atomic operation)
- Falls back gracefully: if the lock cannot be acquired after retries with exponential backoff, the request fails fast with `IdempotencyLockError`

### Transactional Outbox Pattern
- Payment record and outbox record are written in a **single database transaction** — either both exist or neither does
- Eliminates the dual-write problem: the API can return without having called bunq, yet the worker is guaranteed to find the outbox record
- Workers poll with `SELECT FOR UPDATE SKIP LOCKED` — multiple worker instances can run safely without stepping on each other
- Batch fetch returns only IDs, then each ID is processed in its own independent session — prevents the lock-release bug where committing inside a loop would release all `FOR UPDATE` locks from the outer transaction

### Webhook Ingestion & Deduplication
- Webhook receipt is a two-phase operation: store first (`RECEIVED`), enqueue after commit (via `BackgroundTasks`)
- Redis enqueue happens after the DB transaction commits — prevents a Redis push from referencing an event that never committed
- Deduplication enforced at two levels: application-level lookup before DB write, and DB UNIQUE constraint on `event_id` as a race-condition safety net
- bunq RSA webhook signature is verified synchronously before any event is stored or domain logic runs
- Unrecognised payload structures fall back to a SHA-256 content hash as the deduplication key

### Double-Entry Ledger
- Every financial event produces a balanced pair of ledger entries that sum to zero — violation raises `LedgerImbalanceError` before any DB write
- Three entry lifecycles: `record_payment_sent` (DEBIT source / CREDIT suspense), `record_payment_confirmed` (DEBIT suspense / CREDIT external), `record_payment_failed` (DEBIT suspense / CREDIT source — reversal)
- Each write method is independently idempotent: re-running on the same `payment_id` returns the existing `transaction_ref` without writing new entries
- Running balances on `LedgerAccount` are updated atomically with entry insertion (denormalised for performance; ground truth is always the entry rows)
- Account rows are locked in deterministic `id` order with `SELECT FOR UPDATE` before balance updates — prevents deadlocks between concurrent transactions affecting the same pair of accounts

### Concurrency-Safe Processing
- `SELECT FOR UPDATE SKIP LOCKED` throughout — both in the payment worker batch fetch and in the per-record processing session
- Fresh session per outbox record: each payment is processed independently; a failure in one does not roll back others
- Ledger writes use SERIALIZABLE isolation (only for that specific session factory) to protect balance read-then-write sequences from phantom reads
- All other sessions use READ COMMITTED — prevents spurious serialization failures on unrelated read paths (health checks, list queries)

### PostgreSQL-Specific Guarantees
- `INSERT ... ON CONFLICT DO NOTHING` for ledger account creation — handles the race where two concurrent workers both find `None` and attempt to insert the same account
- JSONB payload column with JSON fallback for test environments (SQLite)
- `NUMERIC(18, 2)` for all monetary amounts — never `FLOAT`
- Timezone-aware timestamps throughout (`DateTime(timezone=True)`)
- DB CHECK constraints on `payments.state` and `outbox.status` — ORM bugs or raw SQL writes cannot insert invalid state strings

### Observability & Logging
- Structured JSON logging via `structlog` — every log line is machine-parseable
- Correlation IDs propagated through the full request lifecycle (middleware injects `X-Correlation-ID` if absent)
- Request logging middleware records method, path, status code, and latency on every request
- All domain events log structured extras: `payment_id`, `external_id`, `amount`, `currency`, `bunq_payment_id`, `retry_count`
- Payment state transitions are logged with `from_state` and `to_state` for event-sourcing-style auditability

### Background Workers
- **Payment worker** — polls outbox every 2 seconds, processes PENDING records, calls bunq, writes ledger entries
- **Webhook worker** — dequeues event IDs from Redis, applies domain effects (state transitions, ledger entries)
- **Reconciliation worker** — runs every 5 minutes, compares internal ledger balances with bunq, recovers ambiguous payments
- Each worker runs as a separate process (not an asyncio task in the API process) — crashes are isolated; the API stays up if a worker dies

### Failure Recovery
- **Stuck PROCESSING recovery** — any outbox row in `PROCESSING` for more than `2 × OUTBOX_LOCK_TIMEOUT_SECONDS` is reset to `PENDING` with a retry backoff on every worker tick
- Recovery increments `retry_count` to prevent an infinitely crashing worker from looping forever
- **Ambiguous payment recovery** — payments with `FAILED` state and `AMBIGUOUS` error prefix are checked against bunq by the reconciliation worker; if bunq confirms the payment went through, the payment is recovered to `SUBMITTED → CONFIRMED`
- **Webhook retry** — `RECEIVED` events that were never enqueued (Redis failure after DB commit) are picked up by the webhook worker's RETRY_PENDING scan

### Schema Migrations
- Alembic with auto-generated migration scripts and explicit `upgrade`/`downgrade` paths
- Migration history tracked in the `alembic_version` table
- `env.py` uses the same `Base` metadata as the application — migrations are never out of sync with ORM models

### API Authentication & Security
- `X-API-Key` header authentication enforced by middleware on every non-health request
- API key is loaded from environment at startup; an unconfigured key in non-debug mode raises at boot (fail-fast)
- CORS origins explicitly configured — wildcard `*` is not permitted by default
- Webhook signature verification uses bunq's RSA server public key captured at session bootstrap
- No sensitive data (credentials, IBAN, amounts) is logged at DEBUG level without explicit sanitisation

---

## System Architecture

### High-Level Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              Client / Caller                             │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ X-API-Key  POST /payments
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          FastAPI  (main.py)                              │
│  CorrelationID → APIKey → RequestLogging → CORS                          │
│  /payments  /webhooks/bunq  /health                                      │
└──────┬───────────────────────────────────────────┬───────────────────────┘
       │                                           │
       ▼                                           ▼
┌──────────────┐                        ┌──────────────────────┐
│  Payment     │                        │  Webhook Route       │
│  Service     │                        │  (receive + enqueue) │
│              │                        └────────┬─────────────┘
│  Idempotency │                                 │ BackgroundTask
│  Layer 1-3   │                                 ▼
└──────┬───────┘                        ┌──────────────────────┐
       │  atomic write                  │  Redis Queue         │
       ▼                                │  (webhook event IDs) │
┌──────────────┐                        └────────┬─────────────┘
│  PostgreSQL  │◄───────────────────────────────►│
│  payments    │                                 │
│  outbox      │   ◄──────── poll ─────────────  │
│  ledger      │                        ┌────────┴─────────────┐
│  webhooks    │                        │  Workers (separate   │
└──────────────┘                        │  processes)          │
                                        │                      │
                                        │  payment_worker      │─── bunq API
                                        │  webhook_worker      │
                                        │  reconciliation      │─── bunq API
                                        └──────────────────────┘
```

### Request Lifecycle (POST /payments)

```
Client
  │
  │  POST /payments  {external_id, from_account_id, to_iban, amount, currency}
  ▼
Middleware stack (correlation ID → API key check → request log)
  │
  ▼
PaymentService.create_payment()
  │
  ├─ [Layer 1] Redis GET idem:{external_id}  →  cache hit?  ──► return cached payment
  │
  ├─ [Layer 2] Redis SET NX lock:pay:{external_id}  →  acquired?
  │              │
  │              ├─ [DB check under lock] SELECT * FROM payments WHERE external_id=?
  │              │       →  DB hit?  ──► backfill cache, return existing payment
  │              │
  │              ├─ validate inputs (IBAN MOD-97, currency whitelist, amount precision)
  │              │
  │              └─ BEGIN TRANSACTION
  │                   INSERT INTO payments (state=PENDING)
  │                   INSERT INTO outbox   (status=PENDING, payload={...})
  │                 COMMIT
  │                   [Layer 3] DB UNIQUE constraint guards against lock expiry race
  │
  ├─ Redis SET idem:{external_id} = payment_id  (TTL 24h)
  │
  └─ return 201 PaymentResponse
```

### Payment State Machine

```
                    ┌─────────┐
            ──────► │ PENDING │ ◄────────────────────┐
                    └────┬────┘                       │ retry
                         │ worker picks up            │ (backoff)
                         ▼                            │
                   ┌────────────┐              ┌──────┴──────┐
                   │ PROCESSING │ ─── fail ──► │   FAILED    │
                   └────┬───────┘              └─────────────┘
                         │ bunq accepted                ▲
                         ▼                              │
                   ┌───────────┐                        │
                   │ SUBMITTED │ ─── webhook fail ──────┘
                   └────┬──────┘
                         │ webhook CONFIRMED
                         ▼
                   ┌───────────┐
                   │ CONFIRMED │  (terminal)
                   └───────────┘
```

Terminal states: `CONFIRMED` (no transitions out), `FAILED` after `MAX_PAYMENT_RETRIES` is exhausted.

The state machine is a pure-Python object with no side effects — it only validates transitions and mutates the in-memory `Payment` object. Persistence is always the caller's responsibility, which makes it trivially unit-testable.

### Worker Lifecycle (Payment Worker)

```
Each tick():
  1. _recover_stuck_processing_records()
     └─ SELECT outbox WHERE status=PROCESSING AND updated_at < (now - 2×timeout)
        FOR UPDATE SKIP LOCKED
        → reset to PENDING (or FAILED if max retries exhausted)

  2. _fetch_pending_outbox_ids()
     └─ SELECT id FROM outbox
        WHERE status=PENDING
          AND (next_attempt_at IS NULL OR next_attempt_at <= now())
        LIMIT batch_size
        FOR UPDATE SKIP LOCKED
        → commit immediately (release locks, return IDs only)

  3. For each outbox_id:
     └─ Open fresh SERIALIZABLE session
        SELECT outbox WHERE id=? AND status=PENDING FOR UPDATE SKIP LOCKED
        → if None: another worker claimed it, skip
        SET outbox.status = PROCESSING
        FLUSH
        SELECT payment WHERE id=outbox.payment_id
        PaymentStateMachine.transition_to(PROCESSING)
        BunqPaymentAdapter.create_payment(...)
        → success:
            PaymentStateMachine.transition_to(SUBMITTED, bunq_payment_id=...)
            LedgerEngine.record_payment_sent(...)
            outbox.status = DONE
        → failure:
            _handle_failure() → retry or permanently FAILED
        COMMIT
```

### Webhook Lifecycle

```
bunq pushes event
  │
  ▼
POST /webhooks/bunq
  │
  ├─ Verify X-Bunq-Server-Signature (RSA, bunq server public key)
  │    → 401 if invalid/missing
  │
  ├─ Parse JSON, extract event_id
  │
  ├─ WebhookProcessor.receive()
  │    ├─ Check webhook_events.event_id UNIQUE (DB + app-level)
  │    └─ INSERT INTO webhook_events (status=RECEIVED)
  │
  ├─ COMMIT (get_db dependency exits)
  │
  └─ BackgroundTask: _enqueue_after_commit()
       ├─ Redis RPUSH webhook-queue {event_id}
       └─ UPDATE webhook_events SET status=QUEUED

  ...async...

WebhookWorker.tick()
  ├─ Redis BLPOP webhook-queue
  ├─ SELECT webhook_events WHERE id=?
  ├─ Check processed_webhook_events (idempotency)
  ├─ Parse payload, match to payment by bunq_payment_id
  ├─ PaymentStateMachine.transition_to(CONFIRMED or FAILED)
  ├─ LedgerEngine.record_payment_confirmed() or record_payment_failed()
  ├─ INSERT INTO processed_webhook_events (event_id)
  └─ COMMIT
```

### Eventual Consistency

The system is **eventually consistent** between the API response and the payment's final state. A `POST /payments` call returns immediately with `state: PENDING`. The payment transitions to `SUBMITTED` when the worker processes the outbox record (typically within seconds), and to `CONFIRMED` when bunq sends the confirmation webhook.

This is a deliberate tradeoff: synchronous bunq calls would block the API on external network latency, make retries harder, and couple API availability to bunq's availability. The outbox pattern trades synchronous confirmation for resilience.

Callers should poll `GET /payments/{id}` or consume webhooks to observe final state.

### Exactly-Once vs At-Least-Once

- **Payment submission to bunq**: at-least-once (the outbox worker retries). Bunq deduplication using `bunq_payment_id` lookup prevents double-sends on retry.
- **Ledger writes**: exactly-once via idempotency checks in each `LedgerEngine` method. A retry of `record_payment_sent` returns the existing `transaction_ref` without creating new entries.
- **Webhook processing**: at-least-once delivery from bunq, at-most-once domain processing via `processed_webhook_events` deduplication.

---

## Tech Stack

| Component | Choice | Reasoning |
|---|---|---|
| **API framework** | FastAPI | Native async, automatic OpenAPI, Pydantic integration, dependency injection system well-suited to per-request session management |
| **Database** | PostgreSQL 14 | `FOR UPDATE SKIP LOCKED` for queue semantics, SERIALIZABLE isolation for ledger writes, JSONB for outbox payloads, CHECK constraints, transactional DDL |
| **ORM** | SQLAlchemy 2.x async | True async I/O with asyncpg, `mapped_column` type annotations, `async_sessionmaker`, full async context manager support |
| **Cache / queues** | Redis 7 | `SET NX PX` for distributed locks, list-based queue for webhook events, idempotency key cache |
| **HTTP client** | httpx | Async, supports connection pools, timeout configuration, and request/response inspection — required for bunq's RSA-signed requests |
| **Migrations** | Alembic | Autogenerate from ORM metadata, explicit up/down migrations, transactional DDL support with PostgreSQL |
| **Logging** | structlog | JSON output, bound loggers, context propagation — machine-parseable logs are a production requirement |
| **Settings** | pydantic-settings | Environment variable validation at startup, fails fast on misconfiguration |

**Why not SQLite in production?**

SQLite lacks `FOR UPDATE SKIP LOCKED` (required for queue-safe outbox polling), `SERIALIZABLE` isolation that behaves correctly under concurrency, JSONB, `ON CONFLICT DO NOTHING` with `index_elements`, native UUID types, timezone-aware timestamps, and connection pooling. The test suite uses SQLite with in-memory databases for unit tests that don't touch PostgreSQL-specific features — integration tests require a real PostgreSQL instance.

**Why async IO?**

Payment processing involves multiple external I/O points: PostgreSQL queries, Redis operations, bunq API calls. Async IO lets a single Python process handle many concurrent requests without blocking — particularly important for the outbox worker which holds a PostgreSQL session open while awaiting a bunq HTTP response.

---

## Project Structure

```
.
├── main.py                          # FastAPI application entrypoint (API process only)
├── worker_payment.py                # Payment worker entrypoint (separate process)
├── worker_webhook.py                # Webhook worker entrypoint (separate process)
├── worker_reconciliation.py         # Reconciliation worker entrypoint (separate process)
│
├── app/
│   ├── core/
│   │   ├── config.py                # Pydantic-settings; all env vars validated here at startup
│   │   ├── constants.py             # Enums: PaymentState, OutboxStatus, WebhookEventStatus, etc.
│   │   ├── exceptions.py            # Domain exceptions (DuplicatePaymentError, LedgerImbalanceError, ...)
│   │   └── logging.py               # structlog configuration (JSON / text format switch)
│   │
│   ├── api/
│   │   ├── deps.py                  # FastAPI dependencies: get_db, get_redis, get_correlation_id
│   │   ├── middleware/
│   │   │   ├── auth.py              # X-API-Key header enforcement
│   │   │   ├── correlation.py       # X-Correlation-ID injection/propagation
│   │   │   └── logging.py           # Per-request structured log (method, path, status, latency)
│   │   └── routes/
│   │       ├── payments.py          # POST /payments, GET /payments/{id}, GET /payments
│   │       ├── webhooks.py          # POST /webhooks/bunq
│   │       └── health.py            # GET /health, GET /health/ready
│   │
│   ├── domain/
│   │   ├── payments/
│   │   │   ├── models.py            # Payment ORM model + CreatePaymentRequest + PaymentResponse
│   │   │   ├── service.py           # PaymentService: idempotency, validation, atomic create
│   │   │   ├── state_machine.py     # PaymentStateMachine: pure, no I/O, transition enforcement
│   │   │   └── validators.py        # IBAN MOD-97, currency whitelist, amount precision checks
│   │   ├── ledger/
│   │   │   ├── models.py            # LedgerAccount + LedgerEntry ORM models (append-only)
│   │   │   ├── engine.py            # LedgerEngine: double-entry writes, idempotency, FOR UPDATE
│   │   │   └── invariants.py        # assert_entries_balance(): sum-to-zero enforcement
│   │   └── webhooks/
│   │       ├── models.py            # WebhookEvent + ProcessedWebhookEvent ORM models
│   │       └── processor.py         # WebhookProcessor: receive (fast path) + process (domain)
│   │
│   ├── infrastructure/
│   │   ├── db/
│   │   │   ├── base.py              # SQLAlchemy Base declarative class
│   │   │   ├── session.py           # Engine init, get_session_factory, get_ledger_session_factory
│   │   │   ├── outbox.py            # Outbox ORM model (transactional outbox record)
│   │   │   └── migrations/
│   │   │       ├── env.py           # Alembic env: async engine, metadata from Base
│   │   │       ├── script.py.mako   # Migration file template
│   │   │       └── versions/
│   │   │           ├── 001_initial_schema.py   # payments, outbox, ledger, webhook tables
│   │   │           └── 002_schema_fixes.py     # Index additions, constraint corrections
│   │   ├── bunq/
│   │   │   ├── client.py            # BunqClient: RSA signing, session headers, webhook verify
│   │   │   ├── session_manager.py   # BunqSessionManager: installation, registration, auth token
│   │   │   ├── payments.py          # BunqPaymentAdapter: create_payment(), get_payment()
│   │   │   └── webhook_adapter.py   # Bunq webhook payload parsing and normalisation
│   │   ├── redis/
│   │   │   ├── client.py            # Redis connection init/teardown
│   │   │   ├── locks.py             # DistributedLock: SET NX + Lua release script
│   │   │   └── idempotency.py       # IdempotencyStore: GET/SET with TTL
│   │   └── messaging/
│   │       ├── queue.py             # WebhookQueue: Redis list RPUSH/BLPOP
│   │       └── workers.py           # BaseWorker: poll loop, graceful shutdown, error handling
│   │
│   └── workers/
│       ├── payment_worker.py        # PaymentWorker: outbox poll, bunq submit, ledger write
│       ├── webhook_worker.py        # WebhookWorker: Redis dequeue, domain dispatch
│       └── reconciliation_worker.py # ReconciliationWorker: balance drift, ambiguous recovery
│
├── tests/
│   ├── unit/                        # No I/O. SQLite or pure Python. Fast.
│   │   ├── test_state_machine.py
│   │   ├── test_validators.py
│   │   ├── test_idempotency.py
│   │   ├── test_ledger_invariants.py
│   │   └── test_fixes.py
│   ├── integration/                 # Require real PostgreSQL. Test DB behaviour under concurrency.
│   │   ├── payment/
│   │   │   ├── test_payment_service.py
│   │   │   └── test_failure_modes.py
│   │   ├── ledger/
│   │   │   └── test_ledger_engine.py
│   │   └── webhooks/
│   │       └── test_webhook_processor.py
│   ├── failure_scenarios/           # Crash simulation, duplicate delivery, retry exhaustion
│   │   └── test_failure_modes.py
│   ├── factories/                   # SQLAlchemy model factories for test data
│   └── fixtures/
│       └── conftest.py              # Shared fixtures: async_engine, db_session, mock_redis
│
├── scripts/
│   ├── bootstrap_bunq_session.py    # One-time bunq installation + registration
│   └── reset_sandbox.py             # Reset sandbox state for development
│
├── alembic.ini
├── docker-compose.yml               # PostgreSQL 14 + Redis 7
├── .env.example
├── requirements.txt
└── pytest.ini
```

---

## Database Design

### Tables

#### `payments`
The canonical payment record. `external_id` is the caller-supplied idempotency key with a UNIQUE constraint. `state` is enforced by a DB CHECK constraint in addition to the state machine.

```sql
CREATE TABLE payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     VARCHAR(255) UNIQUE NOT NULL,  -- idempotency key
    from_account_id VARCHAR(255) NOT NULL,
    to_iban         VARCHAR(34)  NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    currency        VARCHAR(3)   NOT NULL,
    description     TEXT,
    state           VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    bunq_payment_id VARCHAR(255),                  -- populated on SUBMITTED
    retry_count     INTEGER NOT NULL DEFAULT 0,    -- observability mirror
    last_error      TEXT,
    correlation_id  VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at    TIMESTAMPTZ,
    CONSTRAINT ck_payments_state
        CHECK (state IN ('PENDING','PROCESSING','SUBMITTED','CONFIRMED','FAILED'))
);
CREATE INDEX ix_payments_external_id ON payments(external_id);
CREATE INDEX ix_payments_state       ON payments(state);
```

#### `outbox`
One record per payment. Drives the async worker. Payload is denormalised so the worker does not need to JOIN to the payment table for the bunq API call.

```sql
CREATE TABLE outbox (
    id              UUID PRIMARY KEY,
    payment_id      UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE UNIQUE,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    payload         JSONB NOT NULL,           -- full payment payload for worker
    retry_count     INTEGER NOT NULL DEFAULT 0,  -- authoritative retry counter
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ,              -- NULL = ready immediately
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_outbox_status
        CHECK (status IN ('PENDING','PROCESSING','DONE','FAILED'))
);
CREATE INDEX ix_outbox_status          ON outbox(status);
CREATE INDEX ix_outbox_payment_id      ON outbox(payment_id);
CREATE INDEX ix_outbox_next_attempt_at ON outbox(next_attempt_at)
    WHERE status = 'PENDING';
```

#### `ledger_accounts`
Mirrors bunq monetary accounts. Running balance is denormalised — updated atomically with each entry batch. Ground truth is always derivable from `ledger_entries`.

#### `ledger_entries`
Append-only. Never updated or deleted after insert. Corrections are reversal entries. Every `transaction_ref` groups a balanced pair (DEBIT + CREDIT that sum to zero).

```sql
CREATE TABLE ledger_entries (
    id              UUID PRIMARY KEY,
    account_id      UUID NOT NULL REFERENCES ledger_accounts(id),
    payment_id      UUID REFERENCES payments(id),
    entry_type      VARCHAR(10) NOT NULL,     -- DEBIT | CREDIT
    amount          NUMERIC(18,2) NOT NULL,
    currency        VARCHAR(3) NOT NULL,
    description     TEXT,
    transaction_ref VARCHAR(64) NOT NULL,     -- groups a balanced pair
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_ledger_entries_account_id      ON ledger_entries(account_id);
CREATE INDEX ix_ledger_entries_payment_id      ON ledger_entries(payment_id);
CREATE INDEX ix_ledger_entries_transaction_ref ON ledger_entries(transaction_ref);
```

#### `webhook_events`
Stores every incoming webhook before processing. `event_id` has a UNIQUE constraint for deduplication.

#### `processed_webhook_events`
Records every successfully processed webhook event. Checked before processing to guarantee at-most-once domain application.

### Transaction Boundaries

Every operation that requires atomicity uses an explicit transaction boundary:

- **Payment + Outbox creation**: single `BEGIN/COMMIT` — both rows exist or neither does
- **Outbox processing** (per record): `BEGIN SERIALIZABLE ... COMMIT` — includes outbox status update, payment state transition, ledger entries, and ledger balance updates
- **Webhook receive**: single `BEGIN/COMMIT` — stores the event; enqueue happens after commit via `BackgroundTasks`
- **Webhook process**: single `BEGIN/COMMIT` — state transition + ledger entry + `processed_webhook_events` insert

### Isolation Levels

- **Default engine**: `READ COMMITTED` — appropriate for all reads, payment creation, webhook receive
- **Ledger writes** (`get_ledger_session_factory()`): `SERIALIZABLE` — required for the read-then-write balance update sequence; prevents phantom reads in edge cases beyond what `SELECT FOR UPDATE` covers
- Previously SERIALIZABLE was applied globally, causing PG error `40001` on unrelated read paths under concurrent load. The fix scopes SERIALIZABLE to exactly the sessions that need it.

---

## Reliability Guarantees

### Idempotency

The `external_id` field functions as a caller-controlled idempotency key. The system guarantees that for any given `external_id`, exactly one payment record will ever be created, and the same response will be returned to any number of callers regardless of retries.

The three-layer strategy (Redis cache → distributed lock → DB UNIQUE constraint) ensures this holds under:
- Sequential retries (handled by Layer 1)
- Concurrent identical requests (handled by Layer 2)
- Redis outage (handled by Layer 3)
- Worker crash and restart (Layer 3 + outbox retry)

### Retries

Retry logic is governed exclusively by `outbox.retry_count`. The payment worker's `_handle_failure()` is the only place that increments it. The state machine does not touch retry counts — it has no authority over scheduling.

Backoff schedule for `MAX_PAYMENT_RETRIES = 5`:

| Attempt | Backoff |
|---|---|
| 1 | 2s |
| 2 | 4s |
| 3 | 8s |
| 4 | 16s |
| 5 | FAILED (permanent) |

Maximum backoff is capped at 300 seconds to prevent indefinite delays on very high retry counts.

### Dead-Letter / Failure Handling

After `MAX_PAYMENT_RETRIES`, the outbox record is set to `status=FAILED` and the payment to `state=FAILED` with `last_error` populated. No automatic recovery happens after this point. Operational intervention is required — either a manual retry (reset outbox to PENDING) or a refund initiated by the application layer.

The reconciliation worker logs `CRITICAL` on any permanent failure that produced a ledger entry (to avoid a situation where money moved but is marked failed).

### Stuck PROCESSING Recovery

If a worker crashes after setting `outbox.status=PROCESSING` but before committing `DONE`, `FAILED`, or `PENDING`, the record is permanently stuck — no future worker tick will pick it up (because `status != PENDING`).

Every tick runs `_recover_stuck_processing_records()` which:
1. Selects rows stuck in `PROCESSING` for more than `2 × OUTBOX_LOCK_TIMEOUT_SECONDS`
2. Resets them to `PENDING` with incremented `retry_count` and backoff scheduling
3. If `retry_count >= MAX_PAYMENT_RETRIES`, marks them permanently `FAILED`

This prevents silent payment loss on worker crash.

### Distributed Locks

The Redis lock prevents concurrent creation of payments with the same `external_id`. Without it, two simultaneous `POST /payments` calls could both pass the Redis cache check, both fail the DB check (simultaneously), and both attempt to insert — racing to the UNIQUE constraint.

The lock uses a unique token per acquisition (not a fixed string) so a process cannot accidentally release a lock it no longer owns after TTL expiry. Release uses a Lua CAS (check-and-set) script executed atomically on the Redis server.

**Important**: this implementation uses single-node Redis `SET NX PX`. For a Redis cluster, Redlock would be needed. The design is explicit about this tradeoff.

### Duplicate Webhook Handling

bunq may deliver the same webhook event multiple times (at-least-once delivery is standard in HTTP-based webhook systems). The system handles this at two levels:

1. `webhook_events.event_id` UNIQUE constraint — prevents the same event being stored twice
2. `processed_webhook_events` check before domain processing — prevents the same event being applied twice even if processing is retried

Without the second guard, a webhook worker crash after a partial domain write (e.g. state transitioned but ledger not yet written) would result in duplicate ledger entries on retry.

### Financial Correctness Guarantees

- All monetary amounts are stored as `NUMERIC(18, 2)` — never floating point
- IBAN MOD-97 checksum validated before any payment is created
- Amount precision validated to exactly 2 decimal places at the Pydantic layer
- Ledger entries checked for balance (sum-to-zero) before any DB write
- Ledger writes are idempotent — replaying on the same `payment_id` is safe
- Running balances are updated in the same transaction as entry insertion — no partial updates
- Account rows locked in deterministic `id` order to prevent deadlocks

---

## API Documentation

### Authentication

All endpoints (except `/health`) require:

```
X-API-Key: <your-api-key>
```

Requests without this header return `401 Unauthorized`. The API key is set via the `API_KEY` environment variable.

### POST /payments

Create a payment or return an existing one for the same `external_id`.

**Request**

```http
POST /payments
Content-Type: application/json
X-API-Key: secret
X-Correlation-ID: req-abc123  (optional; generated if absent)

{
  "external_id": "order-9f3a2c-payment-1",
  "from_account_id": "123456",
  "to_iban": "NL02ABNA0123456789",
  "amount": "42.50",
  "currency": "EUR",
  "description": "Invoice #1042"
}
```

**Response (201 Created)**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "external_id": "order-9f3a2c-payment-1",
  "from_account_id": "123456",
  "to_iban": "NL02ABNA0123456789",
  "amount": "42.50",
  "currency": "EUR",
  "description": "Invoice #1042",
  "state": "PENDING",
  "bunq_payment_id": null,
  "retry_count": 0,
  "correlation_id": "req-abc123",
  "created_at": "2024-01-15T12:00:00Z",
  "updated_at": "2024-01-15T12:00:00Z",
  "confirmed_at": null
}
```

A second request with the same `external_id` returns the same payment (200 OK).

**Validation errors (422)**

| Field | Rule |
|---|---|
| `external_id` | 1–255 characters |
| `to_iban` | MOD-97 checksum must pass |
| `amount` | positive, max 2 decimal places, ≤ MAX_PAYMENT_AMOUNT |
| `currency` | must be in SUPPORTED_CURRENCIES |

### GET /payments/{payment_id}

Retrieve a payment by internal UUID.

```http
GET /payments/550e8400-e29b-41d4-a716-446655440000
X-API-Key: secret
```

Returns `PaymentResponse` (same shape as POST response) or `404` if not found.

### GET /payments

List payments with pagination and optional state filter.

```http
GET /payments?page=1&page_size=20&state=SUBMITTED
X-API-Key: secret
```

```json
{
  "items": [...],
  "total": 147,
  "page": 1,
  "page_size": 20
}
```

Uses a window function (`COUNT(*) OVER ()`) so total count and page data come from the same query snapshot — no separate `COUNT(*)` query that could reflect different state under concurrent inserts.

### POST /webhooks/bunq

Receive webhook events from bunq. This endpoint is called by bunq's infrastructure, not by your application.

```http
POST /webhooks/bunq
X-Bunq-Server-Signature: <RSA-SHA256 signature>
Content-Type: application/json

{ ...bunq event payload... }
```

Returns `{"status": "queued"}` on success. Returns `401` if the signature is missing or invalid.

### Health Endpoints

```http
GET /health
# → {"status": "ok", "version": "1.0.0"}

GET /health/ready
# → {"status": "ready", "database": "ok", "redis": "ok"}
# → 503 if any dependency is unhealthy
```

---

## Running Locally

### Prerequisites

- Python 3.11+
- Docker (for PostgreSQL and Redis)
- A bunq sandbox API key from [bunq developer portal](https://www.bunq.com/en/sandbox)

### Environment Variables

```bash
cp .env.example .env
# Edit .env — at minimum set BUNQ_API_KEY and API_KEY
```

Key variables:

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL async DSN | `postgresql+asyncpg://postgres:postgres@localhost:5432/PaymentServiceBunq` |
| `REDIS_URL` | Redis DSN | `redis://localhost:6379/0` |
| `BUNQ_API_KEY` | bunq sandbox API key | *(required)* |
| `API_KEY` | X-API-Key header value | *(required in non-debug mode)* |
| `LOG_FORMAT` | `json` or `text` | `json` |
| `PAYMENT_WORKER_POLL_INTERVAL` | Worker tick interval (seconds) | `2.0` |

### Start Dependencies

```bash
docker-compose up -d
# Starts PostgreSQL on :5432 and Redis on :6379
```

### Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run Migrations

```bash
alembic upgrade head
```

### Bootstrap bunq Session

The bunq integration requires a one-time installation (RSA keypair generation + API registration):

```bash
python scripts/bootstrap_bunq_session.py
```

### Start the API

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --log-config /dev/null
```

Or with auto-reload for development:

```bash
LOG_FORMAT=text uvicorn main:app --reload
```

### Start Workers

Each worker runs as a separate process:

```bash
python worker_payment.py &
python worker_webhook.py &
python worker_reconciliation.py &
```

### Run Tests

```bash
# Unit tests only (no PostgreSQL required)
pytest tests/unit/ -v

# Integration tests (requires PostgreSQL)
TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/test_payments \
    pytest tests/integration/ -v

# Failure scenario tests
pytest tests/failure_scenarios/ -v

# Full suite with coverage
pytest --cov=app --cov-report=term-missing
```

---

## Testing Strategy

### Unit Tests (`tests/unit/`)

Pure Python — no I/O, no database, no Redis. Run in milliseconds. Cover:

- `PaymentStateMachine` — every valid and invalid transition combination
- `validate_create_payment_request` — IBAN MOD-97, currency whitelist, amount precision edge cases
- `assert_entries_balance` — balanced and imbalanced entry sets
- `IdempotencyStore` — cache hit, miss, TTL behaviour (with `mock_redis`)
- Bug-regression tests (`test_fixes.py`) — documents and tests every specific bug that was found and fixed

### Integration Tests (`tests/integration/`)

Require a real PostgreSQL instance (`TEST_DATABASE_URL`). Use `asyncpg` via SQLAlchemy against a real schema (created with `metadata.create_all()`). These tests verify behaviour that unit tests cannot: constraint violations, `SELECT FOR UPDATE SKIP LOCKED`, window functions, `ON CONFLICT DO NOTHING` race outcomes.

- `test_payment_service.py` — idempotency across concurrent sessions, DB constraint enforcement
- `test_ledger_engine.py` — balance invariants, concurrent balance updates, idempotent re-runs
- `test_webhook_processor.py` — duplicate event detection, domain dispatch, processed-event deduplication

**Why PostgreSQL integration tests matter**: SQLite does not support `FOR UPDATE SKIP LOCKED`, has different JSONB handling, and does not enforce `NUMERIC(18,2)` precision the same way. Any test that exercises queue semantics, ledger correctness, or concurrent writes must run against PostgreSQL to be meaningful.

### Failure Scenario Tests (`tests/failure_scenarios/`)

Simulate the exact failure modes the system is designed to handle:

- Worker crash mid-processing (stuck PROCESSING recovery)
- Duplicate webhook delivery at different times (deduplication correctness)
- Retry exhaustion (permanently FAILED after MAX_PAYMENT_RETRIES)
- Ledger idempotency under simulated replay
- `BunqPaymentAmbiguousError` handling (network timeout after bunq accepts)

### Test Factories (`tests/factories/`)

SQLAlchemy model factories (`payment_factory.py`, `ledger_factory.py`, `webhook_factory.py`) — avoid repetitive fixture setup and make test intent clear. Factories insert directly via the test session so tests can control exact state without going through the service layer.

---

## Failure Scenarios

### Worker Crash Mid-Payment

**Scenario**: Worker sets `outbox.status=PROCESSING`, calls bunq, bunq accepts the payment, the process crashes before writing `status=DONE`.

**Recovery**: Next tick's `_recover_stuck_processing_records()` finds the row (stuck in PROCESSING for > 2×timeout), increments `retry_count`, schedules retry with backoff. On retry, `BunqPaymentAdapter.create_payment()` is called again. If bunq returns the same payment (idempotent on the payment reference), the worker stores `bunq_payment_id` and marks DONE. If bunq rejects as duplicate, the worker uses `get_payment()` to retrieve the existing payment ID.

**Outcome**: Payment submitted exactly once. Ledger entry written exactly once (idempotency check in `record_payment_sent`).

### Duplicate Webhook Delivery

**Scenario**: bunq delivers a `CONFIRMED` webhook event for payment `X`. Before the webhook worker processes it, bunq re-delivers the same event (common in HTTP webhook retry systems).

**Recovery**: First delivery: `webhook_events` insert succeeds, processing runs, `processed_webhook_events` insert succeeds, payment → CONFIRMED, ledger entry written. Second delivery: `webhook_events` UNIQUE constraint fails (duplicate `event_id`), `DuplicateWebhookError` is raised, route returns `200` immediately. No domain logic runs.

**Outcome**: Payment confirmed exactly once. Ledger entry written exactly once.

### Lost Network Response After bunq Accepts

**Scenario**: Worker calls `POST /v1/user/{id}/monetary-account/{id}/payment`. bunq creates the payment internally and sends a `200` response. The network drops before the worker receives the response.

**Result**: `httpx` raises a timeout or connection error. Worker catches it as `BunqPaymentAmbiguousError`. Payment is marked `FAILED` with `last_error = "AMBIGUOUS: ..."`.

**Recovery**: Reconciliation worker (every 5 minutes) finds FAILED payments with `AMBIGUOUS` prefix. For each, calls bunq's balance API and compares with internal ledger. If bunq's balance reflects the outflow, the payment is recovered to `SUBMITTED` (or `CONFIRMED` if webhook already arrived). If no drift, the payment is retried normally.

### Retry Exhaustion

**Scenario**: bunq returns a non-transient error (e.g. insufficient funds, invalid IBAN) on every attempt.

**Recovery**: After `MAX_PAYMENT_RETRIES` (5), `outbox.status=FAILED`, `payment.state=FAILED`. `last_error` contains the final error message. No further automatic processing. Alerting (via structured log `ERROR` event) should trigger operational review. Manual intervention: fix the underlying issue and reset `outbox.status=PENDING` with `retry_count=0`.

### Redis Outage Scenarios

- **Idempotency cache unavailable**: Layer 1 is skipped. Request falls through to Layer 2 (lock). Lock acquisition fails (Redis is down). Request falls through to Layer 3 (DB UNIQUE constraint). New payments can still be created; duplicate detection relies on the DB constraint.
- **Lock unavailable**: Two concurrent identical requests may both reach the DB check. One will hit the UNIQUE constraint and return `DuplicatePaymentError`. This is correct behaviour — it degrades gracefully.
- **Webhook enqueue fails**: `_enqueue_after_commit()` catches the error. The event is stored in `webhook_events` with `status=RECEIVED`. The webhook worker's RETRY_PENDING scan picks up `RECEIVED` events that were never queued, providing automatic recovery.

### DB Deadlocks

The ledger engine locks account rows in deterministic `id` order before updating balances. If Worker A is processing payment P1 (debiting account X, crediting suspense) and Worker B is processing payment P2 (debiting account X, crediting suspense), both lock in the same order (X before suspense), so neither can deadlock with the other. Without deterministic ordering, A→B and B→A locking order would deadlock.

### Serialization Conflicts

Ledger write sessions use SERIALIZABLE isolation. PostgreSQL may return `40001 (serialization failure)` if two concurrent transactions conflict. The worker's outer `try/except` catches this as a generic exception, triggering `_handle_failure()` which schedules a retry with backoff. The retry will succeed because serialization conflicts are transient.

---

## Security Considerations

### API Key Authentication

`X-API-Key` is enforced in middleware before any route handler runs. The key is stored only in environment variables — never in source code or logs. An empty `API_KEY` in non-debug mode causes the process to refuse to start (`assert_api_key_configured()`).

### Secret Management

No secrets are hard-coded. All credentials (`BUNQ_API_KEY`, `API_KEY`, `DATABASE_URL`, `REDIS_URL`) are loaded from environment variables at startup via `pydantic-settings`. In production, these should be injected via a secrets manager (AWS Secrets Manager, HashiCorp Vault, Kubernetes Secrets) — not `.env` files on disk.

### Webhook Signature Verification

bunq signs every webhook request body with its RSA private key. The system verifies this signature using the server public key captured during `POST /installation` (stored as `BUNQ_SERVER_PUBLIC_KEY`). Signature verification runs **before** any event is stored or any domain logic executes — an unverified request cannot affect system state.

Without this check, any party who discovers the webhook endpoint URL could inject fake `CONFIRMED` transitions, causing the system to falsely mark payments as settled.

### PII Considerations

IBANs and account IDs are stored in the database — these are required for payment processing and reconciliation. They are never written to logs at any level. `correlation_id` in logs allows tracing a request without exposing financial data. In a production deployment, the database should be encrypted at rest and access-controlled at the network layer.

### SQL Injection Prevention

All database queries use SQLAlchemy's parameterised query construction — no string interpolation into SQL. Payment state filter validation in `list_payments` validates against the `PaymentState` enum before the value reaches a query, providing defence-in-depth even though parameterisation already prevents injection.

### Replay Attack Prevention

Webhook replay attacks are prevented by `processed_webhook_events` deduplication — a replayed event with the same `event_id` is idempotent. RSA signature verification prevents fabricated events from being accepted at all.

---

## Observability

### Structured Logging

All log output is structured JSON (configurable to human-readable text for development). Every log line includes:

- `timestamp`, `level`, `logger` (auto-injected by structlog)
- `payment_id`, `external_id`, `correlation_id` (injected by service methods)
- Domain event names as the message field: `payment.created`, `payment.state_transition`, `ledger.payment_sent`, `webhook.receive.duplicate`

This makes it trivial to filter, aggregate, and alert on specific event types in any log management platform (Datadog, CloudWatch, Loki, Splunk).

### Metrics

The current implementation produces structured log events that can be converted to metrics via log parsing. Adding Prometheus metrics would be straightforward: a `prometheus-fastapi-instrumentator` for HTTP metrics, and explicit counters/histograms in the worker `tick()` methods for queue depth, processing latency, and retry counts.

### Tracing

Every request carries a `correlation_id` (from `X-Correlation-ID` header or generated). This ID is propagated through service calls and appears in all log lines for that request. Full distributed tracing (OpenTelemetry with trace/span propagation through async boundaries and into the worker processes) is the natural next step.

### Operational Debugging

```bash
# Find all payments in a given state
SELECT * FROM payments WHERE state = 'PROCESSING' ORDER BY updated_at;

# Find stuck outbox records
SELECT * FROM outbox
WHERE status = 'PROCESSING'
  AND updated_at < now() - interval '2 minutes';

# Ledger balance check for an account
SELECT bunq_account_id, balance, currency
FROM ledger_accounts
ORDER BY bunq_account_id;

# Verify double-entry balance for all transactions
SELECT transaction_ref,
       SUM(CASE WHEN entry_type='DEBIT' THEN amount ELSE -amount END) AS net
FROM ledger_entries
GROUP BY transaction_ref
HAVING ABS(SUM(CASE WHEN entry_type='DEBIT' THEN amount ELSE -amount END)) > 0.001;
-- Should return zero rows.

# Recent webhook events
SELECT event_id, status, created_at
FROM webhook_events
ORDER BY created_at DESC
LIMIT 20;
```

---

## Production Improvements / Future Work

### Message Queue (Kafka / SQS)

The current Redis list-based queue provides at-least-once delivery within a single Redis node. For production at scale, replacing it with Kafka or AWS SQS provides: consumer groups, replay, backpressure, exactly-once semantics (Kafka), dead-letter queues, and operational visibility. The `BaseWorker` abstraction makes this a bounded change.

### Circuit Breakers

The bunq API adapter currently retries on all errors. A circuit breaker (e.g. via `aiobreaker`) would open after N consecutive failures, fast-failing requests for a cooldown period rather than exhausting retries on a sustained outage. This prevents a bunq outage from causing all workers to exhaust retries simultaneously.

### Rate Limiting

No rate limiting exists on `POST /payments`. In production, rate limiting should be applied at the API gateway layer (nginx, Kong, AWS API Gateway) and optionally at the application layer using a Redis sliding window counter per API key.

### OpenTelemetry

Full distributed tracing across API process, worker processes, and database calls. Trace propagation through the outbox pattern (store trace context in the outbox payload) enables end-to-end payment lifecycle traces from HTTP request to ledger write.

### Kubernetes Deployment

The separate-process design (API + 3 worker types) maps naturally to Kubernetes Deployments with independent HPA scaling. The API can scale to N replicas without multiplying worker instances. Workers can scale based on queue depth metrics.

### Horizontal Scaling

Multiple payment worker instances are safe today (`FOR UPDATE SKIP LOCKED` handles contention). Multiple API instances are safe (`stateless` HTTP handlers with shared PostgreSQL and Redis). Multiple reconciliation workers would require a distributed lock around each reconciliation pass to prevent duplicate balance checks.

### Audit Tooling

The append-only ledger provides a complete audit trail. Adding a dedicated audit query API (balance at a point in time, transaction history for an account, payment trail for a given `external_id`) would make it operationally useful without raw SQL access.

### Fraud Detection

Payment metadata (amount, IBAN, frequency, account) can feed a fraud scoring service. The current architecture provides natural integration points: in `PaymentService.create_payment()` before the DB write (synchronous, request-blocking) or in the payment worker before calling bunq (async, non-blocking for the API).

### Multi-Region

Ledger writes require a single authoritative PostgreSQL writer (or a distributed transactional database like CockroachDB / Google Spanner). API and worker processes can run across regions pointing at the same writer — or active-active with regional write routing and conflict resolution.

---

## Tradeoffs

### Outbox Polling vs Push

The payment worker polls the database every 2 seconds. This introduces up to 2 seconds of latency between payment creation and bunq submission, and produces constant DB load. The alternative — a push-based system using LISTEN/NOTIFY or a message broker — would reduce latency and DB load but adds operational complexity.

For a payment system (where seconds of latency are acceptable and correctness matters more than throughput), polling is the right default. It is simpler to reason about, easier to debug, and produces less surprising failure modes than LISTEN/NOTIFY (which has subtle reconnect semantics).

### Single Redis Node

The distributed lock uses single-node Redis `SET NX PX`. This means the lock is not resilient to Redis failure or network partition between the application and Redis. For the specific use case (idempotency lock during payment creation), this is acceptable: if Redis is down, lock acquisition fails, and the DB UNIQUE constraint (Layer 3) handles the race. A Redis failure cannot cause duplicate payments — it can only cause some concurrent requests to fail with a 503 that the caller should retry.

For use cases where the lock is the only guard (no DB-level fallback), single-node Redis is insufficient and Redlock across 3–5 nodes would be required.

### SERIALIZABLE Only for Ledger Writes

Applying SERIALIZABLE globally caused serialization failures (`40001`) on health checks and list queries under concurrent load — because there was no retry logic for `40001` on read-only paths. Scoping it to ledger write sessions means the additional protection is applied exactly where the read-then-write pattern exists, and no other code paths are affected.

The tradeoff: the SERIALIZABLE session factory is a separate code path from the standard factory, which adds a small surface area for bugs (e.g. using the wrong factory in a new worker). This is mitigated by explicit naming (`get_ledger_session_factory`) and comments in both factories.

### Denormalised Running Balance

`LedgerAccount.balance` is a denormalised cache of the true balance (derivable from summing `LedgerEntry` rows). This provides O(1) balance lookups for reconciliation and API responses, at the cost of complexity: the balance must be updated atomically with every entry batch, and the update uses `SELECT FOR UPDATE` to prevent concurrent update races.

If the balance ever drifts from the true ledger sum (which it shouldn't, given the atomicity guarantees), reconciliation will detect it. The ground truth is always the entries.

### No Synchronous bunq Call in API

The API returns before bunq has been contacted. This means callers cannot know the bunq payment ID from the initial response — they must poll or consume a webhook. For use cases that require synchronous confirmation, this architecture would need a different design (WebSocket, long-polling, or a synchronous payment path with timeout).

The async design was chosen because it is more resilient (API works even if bunq is slow or down), more horizontally scalable, and more natural for the outbox pattern.

---

## Example Commands

```bash
# ── Start infrastructure
docker-compose up -d

# ── Database
alembic upgrade head
alembic downgrade -1
alembic history --verbose

# ── API
uvicorn main:app --host 0.0.0.0 --port 8000
LOG_FORMAT=text uvicorn main:app --reload  # development

# ── Workers
python worker_payment.py
python worker_webhook.py
python worker_reconciliation.py

# ── Tests
pytest tests/unit/ -v
pytest tests/integration/ -v -m integration
pytest tests/failure_scenarios/ -v -m failure
pytest --cov=app --cov-report=html -q
pytest tests/unit/test_state_machine.py -v -k "test_invalid_transition"

# ── Linting / formatting
ruff check app/ tests/
ruff format app/ tests/
mypy app/ --strict

# ── bunq bootstrap
python scripts/bootstrap_bunq_session.py
python scripts/reset_sandbox.py

# ── Operational queries
psql $DATABASE_URL -c "SELECT state, COUNT(*) FROM payments GROUP BY state;"
psql $DATABASE_URL -c "SELECT status, COUNT(*) FROM outbox GROUP BY status;"
```

---

## Engineering Philosophy

**Financial systems prioritise correctness over throughput.** A payment system that processes 10,000 payments per second but occasionally double-charges or loses a payment is not a payment system — it is a liability. Every architectural decision in this project is made with correctness as the primary constraint, and throughput as a secondary one.

**Idempotency is non-negotiable.** Any operation that moves money must be safe to retry. This means idempotency keys, duplicate detection at every layer, and idempotent side effects (ledger writes that do nothing if already applied). An operation that is "probably idempotent" is not idempotent.

**Explicit failure handling is critical.** The most dangerous failures in payment systems are not the ones that raise exceptions — they are the ones that succeed partially. A payment that bunq accepted but the application does not know about. A ledger entry written but the state transition not committed. This project handles these explicitly: the outbox pattern, stuck-state recovery, reconciliation, and ambiguous payment handling are all direct responses to specific partial-failure scenarios.

**Reliability is designed, not added later.** The idempotency strategy, the outbox pattern, the double-entry ledger, and the distributed locking were built in from the start — not retrofitted after the first production incident. Adding these patterns to an existing system is significantly harder than building them in. The complexity they introduce is genuine and intentional.