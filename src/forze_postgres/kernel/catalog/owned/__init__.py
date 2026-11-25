"""Declared shapes of the tables forze owns, their render and their diff (RFC 0080)."""

from .diff import (
    CatalogRelation,
    DiffOutcome,
    RelationDelta,
    diff_outcome,
    diff_relation,
    read_catalog,
    render_diff,
)
from .documents import document_shape
from .quoting import comment, qualified, quote_ident
from .render import render_sql
from .shapes import (
    BoundOwnedRelation,
    OwnedColumn,
    OwnedIndex,
    OwnedRelationShape,
    Retired,
    is_constant_default,
)

# ----------------------- #

__all__ = [
    "BoundOwnedRelation",
    "CatalogRelation",
    "DiffOutcome",
    "OwnedColumn",
    "OwnedIndex",
    "OwnedRelationShape",
    "RelationDelta",
    "Retired",
    "comment",
    "diff_outcome",
    "diff_relation",
    "document_shape",
    "is_constant_default",
    "qualified",
    "quote_ident",
    "read_catalog",
    "render_diff",
    "render_sql",
]
