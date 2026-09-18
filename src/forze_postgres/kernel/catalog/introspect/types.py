"""Postgres introspection data types and caches."""

from __future__ import annotations

from typing import Literal, final

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


# ....................... #

PostgresColumnTypes = dict[str, PostgresType]
"""Column name to :class:`PostgresType` mapping for one relation."""

PostgresColumnCache = dict[tuple[str, str, str], PostgresColumnTypes]
"""Cache keyed by ``(partition, schema, relation)`` holding column type maps."""

PostgresRelationTriggers = frozenset[str]
"""User-visible trigger names on a relation that fire on UPDATE."""

PostgresTriggerCache = dict[tuple[str, str, str], PostgresRelationTriggers]
"""Cache keyed by ``(partition, schema, relation)`` holding UPDATE trigger names."""

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

    expr: str | None = attrs.field(default=None)
    """Expression extracted from the index definition, if any."""

    columns: tuple[str, ...] = attrs.field(factory=tuple)
    """Tuple of indexed column names (empty for expression-only indexes)."""

    has_tsvector_col: bool = attrs.field(default=False)
    """Whether at least one indexed column has type ``tsvector``."""


# ....................... #

PostgresIndexEngine = Literal["pgroonga", "fts", "unknown"]
"""Classified search engine for an index."""

PostgresIndexCache = dict[tuple[str, str, str], PostgresIndexInfo]
"""Cache keyed by ``(partition, schema, index)`` holding index metadata."""

PostgresIndexDefCache = dict[tuple[str, str, str], str]
"""Cache keyed by ``(partition, schema, index)`` holding raw index definitions."""

# ....................... #

PostgresRelationKind = Literal["table", "view", "materialized_view", "partitioned_table", "other"]
"""Kind of a Postgres relation (table, view, etc.)."""

PostgresRelationCache = dict[tuple[str, str, str], PostgresRelationKind]
"""Cache keyed by ``(partition, schema, relation)`` holding relation kinds."""

PostgresPrimaryKeyCache = dict[tuple[str, str, str], tuple[str, ...]]
"""Cache keyed by ``(partition, schema, relation)`` holding primary-key column names."""

PostgresUniqueColumnSetsCache = dict[tuple[str, str, str], tuple[tuple[str, ...], ...]]
"""Cache keyed by ``(partition, schema, relation)`` holding UNIQUE/PK column sets."""


@attrs.define(slots=True, frozen=True, kw_only=True)
class UniqueIndexInfo:
    """One live UNIQUE index, described in the terms a storage guarantee is checked against.

    Only what the guarantee validation needs, and deliberately not a general index model: the
    key columns (never the ``INCLUDE`` payload, which does not participate in uniqueness), the
    predicate as Postgres deparses it, and whether nulls compare equal.
    """

    columns: frozenset[str]
    """The index's **key** columns, as a set — order does not change which tuples are unique."""

    predicate: str | None
    """``pg_get_expr(indpred, …)`` for a partial index, ``None`` for a full one."""

    nulls_not_distinct: bool
    """Whether the index was built ``NULLS NOT DISTINCT`` — i.e. two nulls conflict."""
