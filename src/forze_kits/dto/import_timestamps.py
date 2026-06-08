"""Opt-in mixin for preserving server-managed timestamps on import/restore.

Create payloads are plain domain-field DTOs; the server stamps ``created_at`` /
``last_update_at`` on write. For faithful import (e.g. restoring from a backup) mix
:class:`ImportTimestamps` into a create payload and use
:meth:`~forze.application.contracts.document.DocumentCommandPort.ensure` — any timestamp the
payload carries flows through to the stored document (via the codec transform), and omitted
ones fall back to the server default. ``rev`` is intentionally not preserved.
"""

from datetime import datetime

from forze.domain.models import BaseDTO

# ----------------------- #


class ImportTimestamps(BaseDTO):
    """Optional ``created_at`` / ``last_update_at`` fields for import-style create payloads."""

    created_at: datetime | None = None
    """Creation timestamp to preserve on import; ``None`` lets the server stamp it."""

    last_update_at: datetime | None = None
    """Last-update timestamp to preserve on import; ``None`` lets the server stamp it."""
