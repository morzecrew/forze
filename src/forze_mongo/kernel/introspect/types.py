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

    partial: bool = attrs.field(default=False)
    """Whether the index covers only some documents.

    True for a ``partialFilterExpression`` and for a ``sparse`` index, which are Mongo's two
    ways of saying the same thing a Postgres ``WHERE`` clause says: this uniqueness applies to
    a subset. Read so a declared *filtered* guarantee is not satisfied by a plain unique index,
    which would refuse every row the guarantee meant to allow."""
