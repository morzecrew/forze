"""Mongo index metadata types."""

from __future__ import annotations

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoIndexInfo:
    """Metadata for one index on a Mongo collection."""

    name: str
    """Index name."""

    keys: tuple[tuple[str, int | str], ...]
    """Indexed fields in order with direction.

    ``1``/``-1`` for ordinary btree indexes, or a string for special index
    types (e.g. ``"text"``, ``"2dsphere"``, ``"hashed"``, ``"vector"``).
    """

    unique: bool = attrs.field(default=False)
    """Whether the index enforces uniqueness."""
