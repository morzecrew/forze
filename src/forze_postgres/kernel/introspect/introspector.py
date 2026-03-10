"""Postgres catalog introspection with in-memory caching."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Optional, cast, final

import attrs
from psycopg import sql

from forze.base.errors import CoreError

from ..platform import PostgresClient
from .types import (
    PostgresColumnCache,
    PostgresColumnTypes,
    PostgresIndexCache,
    PostgresIndexDefCache,
    PostgresIndexEngine,
    PostgresIndexInfo,
    PostgresRelationCache,
    PostgresRelationKind,
    PostgresType,
)
from .utils import extract_index_expr_from_indexdef, normalize_pg_type

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class PostgresIntrospector:
    """Cached introspector that queries Postgres system catalogs.

    Results for relations, columns, and indexes are cached in-memory
    and can be selectively invalidated via :meth:`invalidate_relation`,
    :meth:`invalidate_index`, or fully cleared with :meth:`clear`.
    """

    client: PostgresClient = attrs.field(on_setattr=attrs.setters.frozen)

    # Non initable fields
    __column_cache: PostgresColumnCache = attrs.field(factory=dict, init=False)
    __index_cache: PostgresIndexCache = attrs.field(factory=dict, init=False)
    __relation_cache: PostgresRelationCache = attrs.field(factory=dict, init=False)
    __index_def_cache: PostgresIndexDefCache = attrs.field(factory=dict, init=False)

    # ....................... #

    def __normalize_schema(self, schema: Optional[str]) -> str:
        return schema or "public"

    # ....................... #

    async def get_relation(
        self,
        *,
        schema: Optional[str],
        relation: str,
    ) -> PostgresRelationKind:
        """Return the :data:`PostgresRelationKind` of a relation, using the cache when available.

        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :param relation: Relation (table/view) name.
        :returns: Classified relation kind.
        :raises CoreError: If the relation does not exist.
        """

        schema = self.__normalize_schema(schema)
        key = (schema, relation)

        if key in self.__relation_cache:
            return self.__relation_cache[key]

        stmt = sql.SQL(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = {schema} 
                AND c.relname = {relation}
            LIMIT 1
            """
        ).format(schema=sql.Placeholder(), relation=sql.Placeholder())

        rk = await self.client.fetch_value(stmt, [schema, relation], default=None)

        if rk is None:
            raise CoreError(f"Relation not found: {schema}.{relation}")

        kind: PostgresRelationKind = "other"

        match str(rk):
            case "r":
                kind = "table"

            case "v":
                kind = "view"

            case "m":
                kind = "materialized_view"

            case "p":
                kind = "partitioned_table"

            case _:
                pass

        self.__relation_cache[key] = kind

        return kind

    # ....................... #

    async def require_relation(
        self,
        *,
        schema: Optional[str],
        relation: str,
        allow: tuple[PostgresRelationKind, ...] = (
            "table",
            "view",
            "materialized_view",
            "partitioned_table",
        ),
    ) -> PostgresRelationKind:
        """Assert that a relation exists and its kind is in *allow*.

        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :param relation: Relation name.
        :param allow: Acceptable relation kinds.
        :returns: The validated relation kind.
        :raises CoreError: If the relation kind is not in *allow*.
        """

        kind = await self.get_relation(schema=schema, relation=relation)

        if kind not in allow:
            schema = self.__normalize_schema(schema)

            raise CoreError(
                f"Unsupported relation kind for {schema}.{relation}: {kind} (allowed: {allow})"
            )

        return kind

    # ....................... #

    async def get_column_types(
        self,
        *,
        schema: Optional[str],
        relation: str,
    ) -> PostgresColumnTypes:
        """Return the column type map for a relation, using the cache when available.

        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :param relation: Relation name.
        :returns: Mapping of column names to :class:`PostgresType`.
        :raises CoreError: If the relation does not exist or has no columns.
        """

        schema = self.__normalize_schema(schema)
        key = (schema, relation)

        if key in self.__column_cache:
            return self.__column_cache[key]

        # Fail fast if relation doesn't exist
        await self.require_relation(schema=schema, relation=relation)

        stmt = sql.SQL(
            """
            WITH rel AS (
              SELECT c.oid
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = {schema}
                AND c.relname = {relation}
              LIMIT 1
            )
            SELECT
              a.attnum,
              a.attname AS column_name,
              CASE
                WHEN t.typelem <> 0 THEN format_type(t.typelem, NULL)
                ELSE NULL
              END AS array_elem_type,
              format_type(a.atttypid, a.atttypmod) AS full_type,
              (t.typelem <> 0) AS is_array,
              a.attnotnull AS not_null
            FROM rel
            JOIN pg_attribute a ON a.attrelid = rel.oid
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """
        ).format(schema=sql.Placeholder(), relation=sql.Placeholder())

        rows = await self.client.fetch_all(
            stmt,
            [schema, relation],
            row_factory="dict",
            commit=False,
        )

        if not rows:
            raise CoreError(f"Relation has no columns: {schema}.{relation}")

        out: dict[str, PostgresType] = {}

        for r in rows:
            col = cast(str, r["column_name"])
            is_array = bool(r["is_array"])
            not_null = bool(r["not_null"])

            if is_array:
                elem = r["array_elem_type"]
                if not elem:
                    base = normalize_pg_type(str(r["full_type"]).rstrip("[]"))
                else:
                    base = normalize_pg_type(str(elem))
            else:
                base = normalize_pg_type(str(r["full_type"]))

            out[col] = PostgresType(base=base, is_array=is_array, not_null=not_null)

        self.__column_cache[key] = out

        return out

    # ....................... #

    async def get_index_def(self, *, index: str, schema: Optional[str] = None) -> str:
        """Return the raw ``CREATE INDEX`` definition for an index.

        :param index: Index name.
        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :returns: The full index definition string.
        :raises CoreError: If the index does not exist.
        """

        schema = self.__normalize_schema(schema)
        key = (schema, index)

        if key in self.__index_def_cache:
            return self.__index_def_cache[key]

        stmt = sql.SQL(
            """
            SELECT pg_get_indexdef(c.oid) AS indexdef
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = {schema}
              AND c.relname = {idx}
            LIMIT 1
            """
        ).format(schema=sql.Placeholder(), idx=sql.Placeholder())

        row = await self.client.fetch_one(stmt, [schema, index], row_factory="dict")

        if row is None or not row.get("indexdef"):
            raise CoreError(f"Cannot load indexdef for index: {schema}.{index}")

        indexdef = str(row["indexdef"])
        self.__index_def_cache[key] = indexdef

        return indexdef

    # ....................... #

    async def get_index_info(
        self,
        *,
        index: str,
        schema: Optional[str] = None,
    ) -> PostgresIndexInfo:
        """Return full :class:`PostgresIndexInfo` for an index, classifying its engine.

        Populates both the index cache and the index definition cache.

        :param index: Index name.
        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :returns: Index metadata with classified engine.
        :raises CoreError: If the index does not exist.
        """

        schema = self.__normalize_schema(schema)
        key = (schema, index)

        if key in self.__index_cache:
            return self.__index_cache[key]

        # NOTE:
        # - pg_get_expr(ix.indexprs, ix.indrelid) gives expression for expression indexes
        # - columns extracted from indkey -> pg_attribute
        stmt = sql.SQL(
            """
            SELECT
                am.amname AS amname,
                pg_get_indexdef(i.oid) AS indexdef,
                pg_get_expr(ix.indexprs, ix.indrelid) AS expr,
                array_remove(array_agg(a.attname ORDER BY k.ord), NULL) AS cols,
                COALESCE(bool_or(t.typname = 'tsvector'), false) AS has_tsvector_col
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace in_ ON in_.oid = i.relnamespace
            JOIN pg_am am ON am.oid = i.relam

            LEFT JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
            LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum
            LEFT JOIN pg_type t ON t.oid = a.atttypid

            WHERE in_.nspname = {schema}
                AND i.relname = {idx}
            GROUP BY am.amname, i.oid, ix.indexprs, ix.indrelid
            LIMIT 1
            """
        ).format(schema=sql.Placeholder(), idx=sql.Placeholder())

        row = await self.client.fetch_one(
            stmt,
            [schema, index],
            row_factory="dict",
            commit=False,
        )

        if row is None:
            raise CoreError(f"Index not found: {schema}.{index}")

        amname = str(row.get("amname") or "")
        indexdef = str(row.get("indexdef") or "")
        expr = row.get("expr")
        expr_s = str(expr).strip() if expr is not None else None

        cols_raw: list[Any] = row.get("cols") or []
        # psycopg может вернуть list[str] уже; на всякий случай:
        columns = (
            tuple(str(x) for x in cols_raw)
            if isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
                cols_raw, (list, tuple)
            )
            else ()
        )

        has_tsvector_col = bool(row.get("has_tsvector_col") or False)

        # classify engine
        engine: PostgresIndexEngine = "unknown"
        idx_l = indexdef.lower()

        if amname == "pgroonga":
            engine = "pgroonga"

        elif amname == "gin":
            # Heuristic: gin + tsvector column OR to_tsvector(...) inside expression/DDL
            if has_tsvector_col or ("to_tsvector(" in idx_l) or ("tsvector" in idx_l):
                engine = "fts"

        # fallback expr (ONLY if pg_get_expr didn't return it, but DDL shows expression)
        if expr_s is None:
            maybe = extract_index_expr_from_indexdef(indexdef)
            if maybe:
                expr_s = maybe

        info = PostgresIndexInfo(
            schema=schema,
            name=index,
            amname=amname,
            engine=engine,
            indexdef=indexdef,
            expr=expr_s,
            columns=columns,
            has_tsvector_col=has_tsvector_col,
        )

        self.__index_cache[key] = info
        self.__index_def_cache[key] = indexdef

        return info

    # ....................... #

    def invalidate_relation(self, *, schema: Optional[str], relation: str) -> None:
        """Evict cached relation kind and column types for a specific relation."""

        schema = self.__normalize_schema(schema)
        self.__relation_cache.pop((schema, relation), None)
        self.__column_cache.pop((schema, relation), None)

    # ....................... #

    def invalidate_index(self, *, schema: Optional[str], index: str) -> None:
        """Evict cached index info and definition for a specific index."""

        schema = self.__normalize_schema(schema)
        key = (schema, index)
        self.__index_cache.pop(key, None)
        self.__index_def_cache.pop(key, None)

    # ....................... #

    def clear(self) -> None:
        """Clear all introspection caches."""

        self.__relation_cache.clear()
        self.__column_cache.clear()
        self.__index_cache.clear()
        self.__index_def_cache.clear()
