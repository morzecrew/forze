"""Value objects and shared types for document contracts.

The keyed-write value objects: ``ensure``/``upsert`` insert at a caller-chosen primary key,
and ``update`` patches a known key at an expected revision, so the id is an explicit part of
each item. For bulk variants these value objects bundle the id with its payload(s) and revision
instead of relying on positional tuples (clearer than ``(id, create, update)`` or
``(id, rev, dto)`` triples and extensible — e.g. an import payload can carry its own
``created_at``/``last_update_at``). Also holds the read-side row-lock mode vocabulary.
"""

from typing import Literal
from uuid import UUID

import attrs

from forze.domain.models import BaseDTO

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


# ....................... #


@attrs.define(slots=True, frozen=True)
class KeyedCreate[C: BaseDTO]:
    """A create payload paired with the primary key to insert it at."""

    id: UUID
    """Primary key to insert the payload at."""

    payload: C
    """Create payload (domain fields only)."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class UpsertItem[C: BaseDTO, U: BaseDTO]:
    """A create payload + update payload for one primary key, for bulk upsert."""

    id: UUID
    """Primary key to upsert at."""

    create: C
    """Payload inserted when the key is absent."""

    update: U
    """Patch applied when the key already exists."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class KeyedUpdate[U: BaseDTO]:
    """A patch + its expected revision for one primary key, for bulk update."""

    id: UUID
    """Primary key of the document to update."""

    rev: int
    """Expected revision (optimistic-concurrency token), as in :meth:`update`."""

    dto: U
    """Patch applied to the document."""
