"""Value objects for keyed document writes.

``ensure``/``upsert`` insert at a caller-chosen primary key, so the id is an explicit part
of each item. For bulk variants these value objects bundle the id with its payload(s)
instead of relying on positional tuples (clearer than ``(id, create, update)`` triples and
extensible — e.g. an import payload can carry its own ``created_at``/``last_update_at``).
"""

from uuid import UUID

import attrs

from forze.domain.models import BaseDTO

# ----------------------- #


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
