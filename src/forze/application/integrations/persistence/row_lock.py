"""Adapter-side row-lock degradation logging shared by non-Postgres gateways."""

from forze.application.contracts.document.value_objects import RowLockMode

from ..._logger import logger

# ----------------------- #


def log_non_postgres_lock_degrade(mode: RowLockMode, *, backend: str) -> None:
    """Log when ``nowait`` / ``skip_locked`` is degraded on a non-Postgres backend."""

    if mode in ("nowait", "skip_locked"):
        logger.debug(
            "%s for_update=%r is degraded to transactional read (FOR UPDATE equivalent)",
            backend,
            mode,
        )
