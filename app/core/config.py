"""
app/core/config.py — Application configuration.

All settings are loaded from environment variables (or a .env file via
python-dotenv). No secrets are hard-coded. The Settings object is a
singleton imported everywhere as `from app.core.config import settings`.
"""

from functools import lru_cache
from typing import List

from pydantic import Field, PostgresDsn, RedisDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Server ────────────────────────────────────────────────────────────────
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False
    # FIX: was ["*"] — wildcards allow any origin in production.
    # Set ALLOWED_ORIGINS=["https://your-frontend.example.com"] in production.
    # Empty list disables CORS (safe default for API-only services).
    ALLOWED_ORIGINS: List[str] = []

    # ── Auth ──────────────────────────────────────────────────────────────────
    # FIX: previously missing from config — auth middleware read it via the
    # wrong getattr call and always evaluated to "".
    API_KEY: str = Field(default="", description="API key for X-API-Key header auth")

    # ── Database ──────────────────────────────────────────────────────────────
    DATABASE_URL: PostgresDsn = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/PaymentServiceBunq",
    )
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30

    # ── Redis ─────────────────────────────────────────────────────────────────
    REDIS_URL: RedisDsn = Field(default="redis://localhost:6379/0")
    REDIS_LOCK_TTL_SECONDS: int = 30
    REDIS_IDEMPOTENCY_TTL_SECONDS: int = 86400  # 24 h

    # ── bunq sandbox ──────────────────────────────────────────────────────────
    BUNQ_API_KEY: str = Field(default="", description="bunq sandbox API key")
    BUNQ_ENVIRONMENT: str = "SANDBOX"  # SANDBOX | PRODUCTION
    BUNQ_BASE_URL: str = "https://public-api.sandbox.bunq.com/v1"
    BUNQ_TIMEOUT_SECONDS: int = 30
    BUNQ_MAX_RETRIES: int = 3
    BUNQ_RETRY_BACKOFF_BASE: float = 2.0  # seconds
    # Server public key returned by bunq POST /installation (ServerPublicKey object).
    # Required for webhook signature verification. Set this after bootstrapping.
    # PRODUCTION REQUIREMENT: must be set before go-live.
    BUNQ_SERVER_PUBLIC_KEY: str = Field(
        default="",
        description="bunq server RSA public key PEM for webhook signature verification",
    )

    # ── API limits ────────────────────────────────────────────────────────────
    # Limit incoming webhook body size to prevent memory exhaustion from
    # oversized payloads. bunq payloads are small; 1MB is generous.
    MAX_WEBHOOK_BODY_BYTES: int = 1_048_576  # 1 MB

    # ── Workers ───────────────────────────────────────────────────────────────
    PAYMENT_WORKER_POLL_INTERVAL: float = 2.0  # seconds
    WEBHOOK_WORKER_POLL_INTERVAL: float = 1.0
    RECONCILIATION_INTERVAL: int = 300  # seconds (5 min)
    OUTBOX_BATCH_SIZE: int = 10
    WEBHOOK_BATCH_SIZE: int = 20
    # A PROCESSING outbox row older than 2× this value is considered stuck
    # and will be reset to PENDING by the recovery scan.  Must be greater
    # than the maximum expected time for a bunq API call to complete.
    OUTBOX_LOCK_TIMEOUT_SECONDS: int = 60

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # json | text

    # ── Financial ────────────────────────────────────────────────────────────
    SUPPORTED_CURRENCIES: List[str] = ["EUR", "USD", "GBP"]
    MAX_PAYMENT_AMOUNT: str = "100000.00"  # sandbox safety cap

    @field_validator("BUNQ_ENVIRONMENT")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        allowed = {"SANDBOX", "PRODUCTION"}
        if v.upper() not in allowed:
            raise ValueError(f"BUNQ_ENVIRONMENT must be one of {allowed}")
        return v.upper()

    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"LOG_LEVEL must be one of {allowed}")
        return v
    
    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def fix_database_url(cls, v: str) -> str:
        v = str(v)
        if v.startswith("postgres://"):
            v = v.replace("postgres://", "postgresql+asyncpg://", 1)
        if v.startswith("postgresql://"):
            v = v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
