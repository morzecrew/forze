"""Mongo index metadata types."""

from __future__ import annotations

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoIndexInfo:
    """Metadata for one index on a Mongo collection."""

    name: str
    """Index name."""

    keys: tuple[tuple[str, int], ...]
    """Indexed fields in order with direction (1 or -1)."""

    unique: bool = attrs.field(default=False)
    """Whether the index enforces uniqueness."""
