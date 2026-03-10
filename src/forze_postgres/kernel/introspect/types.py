"""Postgres introspection data types and caches."""

from __future__ import annotations

from typing import Literal, Optional, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresType:
    """Normalized description of a single Postgres column type."""

    base: str
    """Canonical short type name (e.g. ``"int4"``, ``"timestamptz"``)."""

    is_array: bool
    """Whether the column is an array of :attr:`base`."""

    not_null: bool
    """Whether the column carries a ``NOT NULL`` constraint."""


PostgresColumnTypes = dict[str, PostgresType]
"""Column name to :class:`PostgresType` mapping for one relation."""

PostgresColumnCache = dict[tuple[str, str], PostgresColumnTypes]
"""Cache keyed by ``(schema, relation)`` holding column type maps."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresIndexInfo:
    """Metadata for a single Postgres index obtained from system catalogs."""

    schema: str
    """Schema where the index resides."""

    name: str
    """Index name."""

    amname: str
    """Access method name (e.g. ``"btree"``, ``"gin"``, ``"pgroonga"``)."""

    engine: PostgresIndexEngine
    """Classified search engine derived from the access method and definition."""

    indexdef: str
    """Full ``CREATE INDEX`` definition returned by ``pg_get_indexdef``."""

    expr: Optional[str] = None
    """Expression extracted from the index definition, if any."""

    columns: tuple[str, ...] = ()
    """Tuple of indexed column names (empty for expression-only indexes)."""

    has_tsvector_col: bool = False
    """Whether at least one indexed column has type ``tsvector``."""


PostgresIndexEngine = Literal["pgroonga", "fts", "unknown"]
"""Classified search engine for an index."""

PostgresIndexCache = dict[tuple[str, str], PostgresIndexInfo]
"""Cache keyed by ``(schema, index)`` holding index metadata."""

PostgresIndexDefCache = dict[tuple[str, str], str]
"""Cache keyed by ``(schema, index)`` holding raw index definitions."""

# ....................... #

PostgresRelationKind = Literal[
    "table", "view", "materialized_view", "partitioned_table", "other"
]
"""Kind of a Postgres relation (table, view, etc.)."""

PostgresRelationCache = dict[tuple[str, str], PostgresRelationKind]
"""Cache keyed by ``(schema, relation)`` holding relation kinds."""
