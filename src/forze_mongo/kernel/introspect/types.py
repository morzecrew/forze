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

    partial_filter: dict[str, object] | None = attrs.field(default=None)
    """The index's ``partialFilterExpression``, or ``None`` for an index over every document.

    Kept whole rather than reduced to a flag, because which documents the index covers is the
    only thing that decides whether it implements a declared filter: an index restricted to
    archived rows and one restricted to current rows are both "partial" and only one of them
    keeps a guarantee about current rows."""

    sparse: bool = attrs.field(default=False)
    """Whether the index skips documents missing every indexed field.

    Separate from :attr:`partial_filter` because the two are not interchangeable, however
    similar they read. A sparse index still indexes an *explicit* null, and a compound sparse
    index covers a document when **at least one** key is present — so it exempts neither a
    tuple holding a null nor a document the guarantee's filter excludes. Conflating them is how
    a guarantee passes startup against an index that does not keep it."""
