"""
app/infrastructure/messaging/workers.py — Base worker class.

All background workers inherit from BaseWorker, which provides:
  - Graceful shutdown via stop() — sets a flag that the run loop checks
    between ticks, allowing the current tick to complete cleanly before exit.
  - Error isolation: a crash in one loop iteration does not kill the worker.
  - Structured logging of each iteration.
  - Configurable poll interval.

Graceful shutdown design:
  Previously the API called task.cancel() on worker tasks, which could
  interrupt a worker mid-tick — e.g. after calling bunq but before committing
  the result to the DB, producing ambiguous payment states on every restart.

  Workers now run as separate processes (worker_payment.py etc.) and receive
  SIGTERM, which calls worker.stop().  The run loop checks _stop between ticks
  and exits cleanly after the current tick commits.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional, Any

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Abstract base class for background workers.

    Subclasses implement tick() which is called on each poll interval.
    If tick() raises an exception it is logged and the worker continues.

    Shutdown:
      Call stop() to request a clean stop.  The worker finishes its current
      tick and then exits the run loop.  This is safe for financial operations
      because in-flight DB transactions complete before the process exits.
    """

    name: str = "worker"
    poll_interval: float = 5.0

    def __init__(
        self,
        settings: Any = None,
        db: Optional[Any] = None,
        redis: Optional[Any] = None,
    ):
        # settings defaults to None so workers can be instantiated in tests
        # without a full settings object.  Workers that need specific settings
        # values call get_settings() directly inside their methods.
        self.settings = settings or get_settings()
        self.db = db
        self.redis = redis
        self._stop: bool = False

    def stop(self) -> None:
        """
        Signal the worker to stop after the current tick completes.

        Thread-safe: can be called from a signal handler.  The run loop
        checks _stop between ticks (not mid-tick), guaranteeing that any
        open DB transaction will have committed before the loop exits.
        """
        logger.info("%s.stop_requested", self.name)
        self._stop = True

    async def run(self) -> None:
        """
        Main loop.  Runs until stop() is called or CancelledError is raised.
        """
        logger.info("%s.started", self.name)

        try:
            while not self._stop:
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise  # Propagate to allow outer await to handle.
                except Exception as exc:
                    logger.error(
                        "%s.tick_failed",
                        self.name,
                        extra={"error": str(exc)},
                        exc_info=True,
                    )

                if not self._stop:
                    await asyncio.sleep(self.poll_interval)

        except asyncio.CancelledError:
            logger.info("%s.cancelled", self.name)

        logger.info("%s.stopped", self.name)

    @abstractmethod
    async def tick(self) -> None:
        """
        Called on each poll interval.

        Must be idempotent.  After this method returns, any DB work it
        performed must be committed — the graceful shutdown path relies
        on this invariant to guarantee a clean exit.
        """
        pass
