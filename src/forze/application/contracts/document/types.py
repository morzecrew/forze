"""Shared types for document contracts."""

from typing import Literal

from forze.application._logger import logger

# ----------------------- #

RowLockMode = Literal[False, True, "nowait", "skip_locked"]
"""Row lock mode for pessimistic reads.

* ``False`` — no lock.
* ``True`` — lock when the backend supports it (Postgres: ``FOR UPDATE``).
* ``"nowait"`` / ``"skip_locked"`` — Postgres ``FOR UPDATE NOWAIT`` / ``SKIP LOCKED``;
  other backends degrade to ``True`` with a debug log.
"""


def row_lock_requires_transaction(mode: RowLockMode) -> bool:
    """Return whether *mode* implies a transactional read on non-Postgres backends."""

    return mode is not False


def log_non_postgres_lock_degrade(mode: RowLockMode, *, backend: str) -> None:
    """Log when ``nowait`` / ``skip_locked`` is degraded on a non-Postgres backend."""

    if mode in ("nowait", "skip_locked"):
        logger.debug(
            "%s for_update=%r is degraded to transactional read (FOR UPDATE equivalent)",
            backend,
            mode,
        )
