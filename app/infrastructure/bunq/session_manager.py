"""
app/infrastructure/bunq/session_manager.py — bunq session lifecycle manager.

bunq sandbox uses a multi-step authentication flow:
  1. Generate an RSA-2048 keypair.
  2. POST /installation — register the public key, receive an installation
     token AND the server's public key (used for webhook signature verification).
  3. POST /device-server — register this device using the installation token.
  4. POST /session-server — create a session, receive a session token.
  5. Use session token as X-Bunq-Client-Authentication on all subsequent calls.
  6. Sign every request body with the client private key.

Session tokens expire (~1 hour). This manager:
  - Persists the RSA keypair and server public key to the database so they
    survive process restarts. The keypair MUST be reused across re-auths —
    bunq binds the installation to the original public key, and re-authing
    with a different keypair will produce 400s.
  - Re-authenticates automatically when a 401 is received, REUSING the
    persisted keypair (POST /session-server only, no new installation).
  - Uses a distributed Redis lock during re-auth to prevent thundering herd.
  - The bunq_sessions table holds exactly ONE row (id=1), updated in place
    on every re-auth. The previous design keyed on session_token which grew
    the table by one row per re-auth indefinitely.
"""

import asyncio
import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.core.constants import REDIS_REAUTH_LOCK_KEY, REDIS_SESSION_KEY
from app.core.exceptions import BunqSessionError
from app.infrastructure.db.models import BunqSession  # noqa: F401 — re-exported for callers
from app.infrastructure.db.session import get_session_factory
from app.infrastructure.redis.client import get_redis_client
from app.infrastructure.redis.locks import DistributedLock

logger = logging.getLogger(__name__)

# Module-level token cache — avoids DB lookup on every request.
_cached_session_token: Optional[str] = None

# Process-level singleton. All workers and the API share one keypair + token.
_session_manager_singleton: Optional["BunqSessionManager"] = None

# Sentinel row ID for the single-row bunq_sessions table.
_SINGLETON_ROW_ID = 1


def get_session_manager() -> "BunqSessionManager":
    """Return the process-level BunqSessionManager singleton."""
    global _session_manager_singleton
    if _session_manager_singleton is None:
        _session_manager_singleton = BunqSessionManager()
    return _session_manager_singleton


class BunqSessionManager:
    """
    Manages the full bunq session lifecycle.

    Keypair lifecycle
    -----------------
    The RSA keypair is generated ONCE during the very first installation and
    persisted to the database. All subsequent re-authentications reload the
    same keypair from the DB and call POST /session-server only — they do NOT
    call POST /installation again, because bunq binds the installation to the
    original public key. Generating a new keypair per re-auth would break the
    integration silently in production.

    Webhook verification
    --------------------
    The server public key returned by bunq's POST /installation response is
    stored alongside the private key. BunqClient uses it to verify the
    X-Bunq-Server-Signature header on incoming webhooks.

    Thread safety
    -------------
    Re-auth is guarded by a Redis distributed lock so only one worker
    executes the auth flow at a time. All others wait and pick up the new
    token from the shared Redis cache.
    """

    BASE_URL = get_settings().BUNQ_BASE_URL
    SESSION_DURATION_HOURS = 1  # bunq sandbox sessions last ~1 hour.

    def __init__(self) -> None:
        # Loaded from DB at bootstrap; never regenerated unless the DB row
        # does not exist (first-ever startup).
        self._private_key: Optional[RSAPrivateKey] = None
        self._public_key_pem: Optional[str] = None
        self._server_public_key_pem: Optional[str] = None

    # ── Public API ────────────────────────────────────────────────────────────

    async def bootstrap(self) -> str:
        """
        Ensure a valid session exists. Called once at startup.

        Strategy:
          1. Load persisted keypair + session from DB.
          2a. If valid session exists: warm in-memory + Redis caches and return.
          2b. If keypair exists but session expired: call POST /session-server
              only (reuse the existing installation — do NOT re-install).
          2c. If no keypair: perform the full installation flow, persist keypair
              and server public key, then create a session.
        """
        global _cached_session_token

        record = await self._load_record_from_db()

        if record:
            # Always reload the keypair from DB — even if the session is still
            # valid — so the in-memory state is consistent with the persisted
            # installation that bunq knows about.
            if record.private_key_pem:
                self._load_keypair_from_pem(record.private_key_pem)
            if record.server_public_key_pem:
                self._server_public_key_pem = record.server_public_key_pem

            if record.expires_at and record.expires_at > datetime.now(timezone.utc):
                # Session still valid — use it directly.
                _cached_session_token = record.session_token
                redis = await get_redis_client()
                ttl = int(
                    (record.expires_at - datetime.now(timezone.utc)).total_seconds()
                )
                if ttl > 0:
                    await redis.set(REDIS_SESSION_KEY, record.session_token, ex=ttl)
                logger.info("bunq.session.loaded_from_db")
                return record.session_token

            if record.installation_token and self._private_key:
                # Keypair exists but session is expired — create a new session
                # WITHOUT re-running the installation flow.
                logger.info("bunq.session.refreshing_expired_session")
                async with httpx.AsyncClient(
                    base_url=self.BASE_URL,
                    timeout=get_settings().BUNQ_TIMEOUT_SECONDS,
                ) as client:
                    new_token = await self._create_session(
                        client, record.installation_token
                    )
                await self._persist_session(
                    new_token, installation_token=record.installation_token
                )
                _cached_session_token = new_token
                redis = await get_redis_client()
                await redis.set(
                    REDIS_SESSION_KEY, new_token, ex=self.SESSION_DURATION_HOURS * 3600
                )
                logger.info("bunq.session.refreshed")
                return new_token

        # No record or no keypair — perform the full installation flow.
        logger.info("bunq.session.full_installation")
        installation_token, session_token = await self._perform_full_auth()
        await self._persist_session(
            session_token, installation_token=installation_token
        )
        _cached_session_token = session_token
        redis = await get_redis_client()
        await redis.set(
            REDIS_SESSION_KEY, session_token, ex=self.SESSION_DURATION_HOURS * 3600
        )
        logger.info("bunq.session.bootstrapped")
        return session_token

    async def get_session_token(self) -> str:
        """
        Return the current valid session token.

        Fast path: in-memory cache.
        Fallback: Redis, then DB + possible re-auth.
        """
        global _cached_session_token

        if _cached_session_token:
            return _cached_session_token

        try:
            redis = await get_redis_client()
            token = await redis.get(REDIS_SESSION_KEY)
            if token:
                _cached_session_token = token
                return token
        except Exception:
            pass

        return await self.bootstrap()

    async def invalidate_and_refresh(self) -> str:
        """
        Called when a 401 is received from bunq.

        Uses a distributed lock so only one worker re-authenticates.
        Others wait, then pick up the new token from the shared cache.
        Reuses the existing installation keypair — never generates a new one.
        """
        global _cached_session_token
        _cached_session_token = None

        redis = await get_redis_client()
        lock = DistributedLock(redis)

        async with lock.acquire(REDIS_REAUTH_LOCK_KEY, timeout=30) as acquired:
            if not acquired:
                await asyncio.sleep(2)
                return await self.get_session_token()

            # Check if another worker already refreshed while we waited.
            token = await redis.get(REDIS_SESSION_KEY)
            if token:
                _cached_session_token = token
                return token

            logger.info("bunq.session.re-authenticating")
            record = await self._load_record_from_db()
            if (
                not record
                or not record.installation_token
                or not record.private_key_pem
            ):
                raise BunqSessionError(
                    "Cannot re-authenticate: no persisted installation found. "
                    "Run bootstrap() first."
                )

            self._load_keypair_from_pem(record.private_key_pem)
            if record.server_public_key_pem:
                self._server_public_key_pem = record.server_public_key_pem

            async with httpx.AsyncClient(
                base_url=self.BASE_URL,
                timeout=get_settings().BUNQ_TIMEOUT_SECONDS,
            ) as client:
                new_token = await self._create_session(
                    client, record.installation_token
                )

            await self._persist_session(
                new_token, installation_token=record.installation_token
            )
            _cached_session_token = new_token
            await redis.set(
                REDIS_SESSION_KEY, new_token, ex=self.SESSION_DURATION_HOURS * 3600
            )
            logger.info("bunq.session.re-authenticated")
            return new_token

    def get_server_public_key_pem(self) -> Optional[str]:
        """Return the bunq server public key PEM for webhook verification."""
        return self._server_public_key_pem

    def sign_request_body(self, body: str) -> str:
        """
        Sign a request body with the client private key.

        bunq requires every mutating request body to be signed with RSA-SHA256.
        The signature is base64-encoded and sent in X-Bunq-Client-Signature.

        Raises BunqSessionError if the private key is not loaded.
        """
        if not self._private_key:
            raise BunqSessionError(
                "Cannot sign request: private key not loaded. Call bootstrap() first."
            )
        signature = self._private_key.sign(
            body.encode("utf-8"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    async def is_session_valid(self) -> bool:
        """Check whether there is a valid non-expired session in the DB."""
        try:
            record = await self._load_record_from_db()
            if not record:
                return False
            return bool(
                record.expires_at and record.expires_at > datetime.now(timezone.utc)
            )
        except Exception:
            return False

    # ── Auth flow ─────────────────────────────────────────────────────────────

    async def _perform_full_auth(self) -> tuple[str, str]:
        """
        Execute the full bunq installation + session flow.

        Returns (installation_token, session_token).
        Generates a new RSA keypair only if one does not already exist.
        """
        if not self._private_key:
            self._generate_keypair()

        async with httpx.AsyncClient(
            base_url=self.BASE_URL,
            timeout=get_settings().BUNQ_TIMEOUT_SECONDS,
        ) as client:
            installation_token = await self._install(client)
            await self._register_device(client, installation_token)
            session_token = await self._create_session(client, installation_token)

        return installation_token, session_token

    def _generate_keypair(self) -> None:
        """Generate RSA-2048 keypair. Called once per installation lifetime."""
        self._private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        self._public_key_pem = (
            self._private_key.public_key()
            .public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            .decode()
        )

    def _load_keypair_from_pem(self, private_key_pem: str) -> None:
        """Deserialise a persisted PEM private key back into the in-memory state."""
        key = serialization.load_pem_private_key(
            private_key_pem.encode("utf-8"),
            password=None,
        )
        if not isinstance(key, rsa.RSAPrivateKey):
            raise BunqSessionError("Loaded private key is not an RSA private key.")
        self._private_key = key
        self._public_key_pem = (
            self._private_key.public_key()
            .public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            .decode()
        )

    async def _install(self, client: httpx.AsyncClient) -> str:
        """POST /installation — register public key, capture server public key."""
        response = await client.post(
            "/installation",
            json={"client_public_key": self._public_key_pem},
        )
        self._raise_for_bunq_error(response, "installation")
        data = response.json()
        installation_token = None
        for item in data.get("Response", []):
            if "Token" in item:
                installation_token = item["Token"]["token"]
            if "ServerPublicKey" in item:
                # Persist the server public key so BunqClient can verify
                # webhook signatures (X-Bunq-Server-Signature).
                self._server_public_key_pem = item["ServerPublicKey"][
                    "server_public_key"
                ]
        if not installation_token:
            raise BunqSessionError("Installation response did not contain a Token.")
        return installation_token

    async def _register_device(
        self, client: httpx.AsyncClient, installation_token: str
    ) -> None:
        """POST /device-server — register this device."""
        response = await client.post(
            "/device-server",
            json={
                "description": "bunq-payment-orchestrator",
                "secret": get_settings().BUNQ_API_KEY,
                "permitted_ips": ["*"],
            },
            headers={"X-Bunq-Client-Authentication": installation_token},
        )
        self._raise_for_bunq_error(response, "device-server")

    async def _create_session(
        self, client: httpx.AsyncClient, installation_token: str
    ) -> str:
        """POST /session-server — create a session and receive session token."""
        response = await client.post(
            "/session-server",
            json={"secret": get_settings().BUNQ_API_KEY},
            headers={"X-Bunq-Client-Authentication": installation_token},
        )
        self._raise_for_bunq_error(response, "session-server")
        data = response.json()
        for item in data.get("Response", []):
            if "Token" in item:
                return item["Token"]["token"]
        raise BunqSessionError("Session-server response did not contain a Token.")

    # ── Persistence ───────────────────────────────────────────────────────────

    async def _persist_session(
        self,
        token: str,
        *,
        installation_token: Optional[str] = None,
    ) -> None:
        """
        Upsert the active session into the single-row bunq_sessions table.

        Always updates id=1 in place. Stores the private key PEM and server
        public key PEM alongside the session token so they survive restarts.
        The old design keyed on session_token, growing the table by one row
        per re-auth indefinitely.
        """
        expires = datetime.now(timezone.utc) + timedelta(
            hours=self.SESSION_DURATION_HOURS
        )
        private_key_pem = None
        if self._private_key:
            # PRODUCTION SECURITY REQUIREMENT: private_key_pem is stored as
            # plaintext PEM in the database. Before go-live this MUST be
            # replaced with KMS-backed envelope encryption (e.g. AWS KMS,
            # GCP Cloud KMS, or HashiCorp Vault). Storing an unencrypted RSA
            # private key in the DB means any DB read access = full API key
            # compromise. See migrations/versions/002_schema_fixes.py for the
            # implementation guide.
            private_key_pem = self._private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ).decode("utf-8")

        values: dict = {
            "id": _SINGLETON_ROW_ID,
            "session_token": token,
            "expires_at": expires,
        }
        if private_key_pem:
            values["private_key_pem"] = private_key_pem
        if self._server_public_key_pem:
            values["server_public_key_pem"] = self._server_public_key_pem
        if installation_token:
            values["installation_token"] = installation_token

        session_factory = get_session_factory()
        async with session_factory() as db:
            stmt = (
                pg_insert(BunqSession)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "session_token": token,
                        "expires_at": expires,
                        **(
                            {}
                            if not private_key_pem
                            else {"private_key_pem": private_key_pem}
                        ),
                        **(
                            {}
                            if not self._server_public_key_pem
                            else {"server_public_key_pem": self._server_public_key_pem}
                        ),
                        **(
                            {}
                            if not installation_token
                            else {"installation_token": installation_token}
                        ),
                    },
                )
            )
            await db.execute(stmt)
            await db.commit()

    async def _load_record_from_db(self) -> Optional[BunqSession]:
        """Load the singleton bunq_sessions row (id=1), or None if absent."""
        try:
            session_factory = get_session_factory()
            async with session_factory() as db:
                result = await db.execute(
                    select(BunqSession).where(BunqSession.id == _SINGLETON_ROW_ID)
                )
                return result.scalar_one_or_none()
        except Exception as exc:
            logger.warning("bunq.session.db_load_failed", extra={"error": str(exc)})
            return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _raise_for_bunq_error(response: httpx.Response, step: str) -> None:
        if response.status_code not in (200, 201):
            raise BunqSessionError(
                f"bunq {step} failed: HTTP {response.status_code} — {response.text[:300]}"
            )
