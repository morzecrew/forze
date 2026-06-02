"""Postgres catalog introspection with in-memory caching."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import hashlib
import json
from datetime import timedelta
from typing import Any, Callable, Sequence, TypeVar, cast, final

import attrs
from psycopg import sql

from forze.base.exceptions import exc
from forze.base.primitives import CachedInflightLane, CacheLane, InflightLane, JsonDict
from forze_postgres.kernel.client import PostgresClientPort

from .types import (
    PostgresColumnTypes,
    PostgresIndexEngine,
    PostgresIndexInfo,
    PostgresRelationKind,
    PostgresRelationTriggers,
    PostgresType,
)
from .utils import extract_index_expr_from_indexdef, normalize_pg_type

# ----------------------- #

CacheKey = tuple[str, str, str]
T_co = TypeVar("T_co")

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class PostgresIntrospector:
    """Cached introspector that queries Postgres system catalogs.

    Results for relations, columns, and indexes are cached in-memory
    and can be selectively invalidated via :meth:`invalidate_relation`,
    :meth:`invalidate_index`, or fully cleared with :meth:`clear`.

    When :attr:`cache_ttl` is set, cached entries expire after that duration
    (monotonic clock) and are transparently reloaded on the next access.
    """

    client: PostgresClientPort = attrs.field(on_setattr=attrs.setters.frozen)
    """Database client used for catalog queries."""

    cache_partition_key: Callable[[], str | None] | None = None
    """When set, scope cache keys by the returned string (e.g. current tenant id).

    Use with database-per-tenant routing so relation and index metadata cached
    for one tenant are not reused for another. If this is set and the callable
    returns ``None``, introspection raises :class:`exc.internal`.
    """

    cache_ttl: timedelta | None = None
    """When set, drop cached relation, column, and index entries after this duration.

    Uses a monotonic clock per process. ``None`` keeps entries until
    :meth:`invalidate_relation`, :meth:`invalidate_index`, or :meth:`clear`.
    """

    max_cache_entries_per_kind: int | None = None
    """When set, cap each of the relation, column, and index caches at this many keys.

    When a cap is exceeded, the oldest inserted key in that cache is evicted
    (FIFO by insertion order). ``None`` means unbounded per-kind growth.
    """

    # ....................... #

    __inflight: InflightLane[Any] = attrs.field(factory=InflightLane, init=False)
    __coalesce: CachedInflightLane[CacheKey, Any] = attrs.field(
        factory=CachedInflightLane, init=False
    )
    __relation_lane: CacheLane[CacheKey, PostgresRelationKind] = attrs.field(init=False)
    __column_lane: CacheLane[CacheKey, PostgresColumnTypes] = attrs.field(init=False)
    __trigger_lane: CacheLane[CacheKey, PostgresRelationTriggers] = attrs.field(
        init=False,
    )
    __pk_lane: CacheLane[CacheKey, tuple[str, ...]] = attrs.field(init=False)
    __unique_sets_lane: CacheLane[CacheKey, tuple[tuple[str, ...], ...]] = attrs.field(
        init=False,
    )
    __index_lane: CacheLane[CacheKey, PostgresIndexInfo] = attrs.field(init=False)
    __row_estimate_lane: CacheLane[CacheKey, int] = attrs.field(init=False)
    __filtered_row_estimate_lane: CacheLane[CacheKey, int] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        mx = self.max_cache_entries_per_kind

        if mx is not None and mx < 1:
            raise exc.configuration(
                "max_cache_entries_per_kind must be at least 1 when set"
            )

        ttl = self._ttl_seconds()

        self.__relation_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )
        self.__column_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )
        self.__trigger_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )
        self.__pk_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )
        self.__unique_sets_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )
        self.__index_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )
        self.__row_estimate_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )
        self.__filtered_row_estimate_lane = CacheLane(
            max_entries=mx,
            ttl_seconds=ttl,
        )

    # ....................... #

    def __normalize_schema(self, schema: str | None) -> str:
        return schema or "public"

    # ....................... #

    def _partition(self) -> str:
        if self.cache_partition_key is None:
            return ""

        p = self.cache_partition_key()

        if p is None:
            raise exc.internal(
                "Postgres introspection requires a cache partition (e.g. tenant id)",
                code="introspection_partition_required",
            )

        return p

    # ....................... #

    def _rel_key(self, schema: str, relation: str) -> CacheKey:
        return (self._partition(), schema, relation)

    def _idx_key(self, schema: str, index: str) -> CacheKey:
        return (self._partition(), schema, index)

    # ....................... #

    def _ttl_seconds(self) -> float | None:
        if self.cache_ttl is None:
            return None

        return self.cache_ttl.total_seconds()

    # ....................... #

    async def _fetch_relation_kind(
        self,
        schema: str,
        relation: str,
        key: CacheKey,
    ) -> PostgresRelationKind:
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
            raise exc.internal(f"Relation not found: {schema}.{relation}")

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

        self.__relation_lane.store(key, kind)

        return kind

    # ....................... #

    async def get_relation(
        self,
        *,
        schema: str | None,
        relation: str,
    ) -> PostgresRelationKind:
        """Return the :data:`PostgresRelationKind` of a relation, using the cache when available.

        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :param relation: Relation (table/view) name.
        :returns: Classified relation kind.
        :raises exc.internal: If the relation does not exist.
        """

        schema = self.__normalize_schema(schema)
        key = self._rel_key(schema, relation)

        return await self.__coalesce.coalesce(
            inflight_key=("pg_rel_kind", *key),
            cache_key=key,
            lane=self.__relation_lane,
            factory=lambda: self._fetch_relation_kind(schema, relation, key),
        )

    # ....................... #

    async def require_relation(
        self,
        *,
        schema: str | None,
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
        :raises exc.internal: If the relation kind is not in *allow*.
        """

        kind = await self.get_relation(schema=schema, relation=relation)

        if kind not in allow:
            schema = self.__normalize_schema(schema)

            raise exc.internal(
                f"Unsupported relation kind for {schema}.{relation}: {kind} (allowed: {allow})"
            )

        return kind

    # ....................... #

    async def _fetch_column_types_uncached(
        self,
        schema: str,
        relation: str,
        key: CacheKey,
    ) -> PostgresColumnTypes:
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
            raise exc.internal(f"Relation has no columns: {schema}.{relation}")

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

        self.__column_lane.store(key, out)

        return out

    # ....................... #

    async def get_column_types(
        self,
        *,
        schema: str | None,
        relation: str,
    ) -> PostgresColumnTypes:
        """Return the column type map for a relation, using the cache when available.

        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :param relation: Relation name.
        :returns: Mapping of column names to :class:`PostgresType`.
        :raises exc.internal: If the relation does not exist or has no columns.
        """

        schema = self.__normalize_schema(schema)
        key = self._rel_key(schema, relation)

        return await self.__coalesce.coalesce(
            inflight_key=("pg_col_types", *key),
            cache_key=key,
            lane=self.__column_lane,
            factory=lambda: self._fetch_column_types_uncached(schema, relation, key),
        )

    # ....................... #

    async def _fetch_unique_index_columns_uncached(
        self,
        schema: str,
        relation: str,
        key: CacheKey,
    ) -> tuple[tuple[str, ...], ...]:
        """Load PK and UNIQUE column sets (plain btree columns only, no partial/expression)."""

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
              i.indisprimary AS is_primary,
              (
                SELECT COALESCE(
                  array_agg(a.attname ORDER BY u.ord),
                  ARRAY[]::text[]
                )
                FROM unnest(i.indkey) WITH ORDINALITY AS u(attnum, ord)
                JOIN pg_attribute a
                  ON a.attrelid = i.indrelid
                 AND a.attnum = u.attnum
                 AND NOT a.attisdropped
                WHERE u.attnum <> 0
              ) AS columns,
              EXISTS (
                SELECT 1 FROM unnest(i.indkey) AS k(attnum) WHERE k.attnum = 0
              ) AS has_expr_key,
              (i.indpred IS NOT NULL) AS is_partial,
              (i.indexprs IS NOT NULL) AS has_indexprs
            FROM pg_index i
            JOIN rel ON rel.oid = i.indrelid
            WHERE i.indisunique
            """
        ).format(schema=sql.Placeholder(), relation=sql.Placeholder())

        rows = await self.client.fetch_all(
            stmt,
            [schema, relation],
            row_factory="dict",
            commit=False,
        )

        sets: list[tuple[str, ...]] = []
        pk: tuple[str, ...] = ()

        for r in rows:
            if r.get("is_partial") or r.get("has_expr_key") or r.get("has_indexprs"):
                if r.get("is_primary"):
                    pk = ()

                continue

            raw_cols: list[str] = r.get("columns") or []
            cols = tuple(str(c) for c in raw_cols)

            if not cols:
                if r.get("is_primary"):
                    pk = ()
                continue

            sets.append(cols)

            if r.get("is_primary"):
                pk = cols

        unique_sets = tuple(sets)
        self.__unique_sets_lane.store(key, unique_sets)
        self.__pk_lane.store(key, pk)

        return unique_sets

    # ....................... #

    async def _load_unique_index_columns(
        self,
        *,
        schema: str,
        relation: str,
    ) -> tuple[tuple[str, ...], ...]:
        schema = self.__normalize_schema(schema)
        key = self._rel_key(schema, relation)

        hit = self.__unique_sets_lane.lookup(key)

        if hit is not None:
            return hit

        self.__pk_lane.invalidate(key)

        return await self.__inflight.run(
            ("pg_unique_sets", *key),
            lambda: self._fetch_unique_index_columns_uncached(schema, relation, key),
        )

    # ....................... #

    async def get_primary_key_columns(
        self,
        *,
        schema: str | None,
        relation: str,
    ) -> tuple[str, ...]:
        """Return primary-key column names in index order (empty when unmappable).

        Expression, partial, or missing primary indexes yield an empty tuple.
        """

        schema = self.__normalize_schema(schema)
        key = self._rel_key(schema, relation)
        hit = self.__pk_lane.lookup(key)

        if hit is not None:
            return hit

        await self._load_unique_index_columns(schema=schema, relation=relation)

        return self.__pk_lane.lookup(key) or ()

    # ....................... #

    async def constraint_exists_for_columns(
        self,
        *,
        schema: str | None,
        relation: str,
        columns: tuple[str, ...],
    ) -> bool:
        """Return whether a non-partial, column-only UNIQUE or PRIMARY KEY matches *columns*."""

        if not columns:
            return False

        schema = self.__normalize_schema(schema)
        sets = await self._load_unique_index_columns(schema=schema, relation=relation)

        return columns in sets

    # ....................... #

    async def _fetch_relation_update_triggers_uncached(
        self,
        schema: str,
        relation: str,
        key: CacheKey,
    ) -> PostgresRelationTriggers:
        await self.require_relation(schema=schema, relation=relation)

        stmt = sql.SQL(
            """
            SELECT t.tgname AS trigger_name
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = {schema}
              AND c.relname = {relation}
              AND NOT t.tgisinternal
              AND (t.tgtype & 2) <> 0
            """
        ).format(schema=sql.Placeholder(), relation=sql.Placeholder())

        rows = await self.client.fetch_all(
            stmt,
            [schema, relation],
            row_factory="dict",
        )
        names = frozenset(str(r["trigger_name"]) for r in rows)

        self.__trigger_lane.store(key, names)

        return names

    # ....................... #

    async def get_relation_update_triggers(
        self,
        *,
        schema: str | None,
        relation: str,
    ) -> PostgresRelationTriggers:
        """Return user-visible trigger names on *relation* that fire on UPDATE."""

        schema = self.__normalize_schema(schema)
        key = self._rel_key(schema, relation)

        return await self.__coalesce.coalesce(
            inflight_key=("pg_rel_triggers", *key),
            cache_key=key,
            lane=self.__trigger_lane,
            factory=lambda: self._fetch_relation_update_triggers_uncached(
                schema,
                relation,
                key,
            ),
        )

    # ....................... #

    def _index_def_stub(
        self,
        *,
        schema: str,
        index: str,
        indexdef: str,
    ) -> PostgresIndexInfo:
        return PostgresIndexInfo(
            schema=schema,
            name=index,
            amname="",
            engine="unknown",
            indexdef=indexdef,
            expr=None,
            columns=(),
            has_tsvector_col=False,
        )

    # ....................... #

    async def _fetch_index_def_uncached(
        self,
        schema: str,
        index: str,
        key: CacheKey,
    ) -> str:
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
            raise exc.internal(f"Cannot load indexdef for index: {schema}.{index}")

        indexdef = str(row["indexdef"])
        existing = self.__index_lane.lookup(key)

        if existing is not None and existing.amname:
            self.__index_lane.store(
                key,
                attrs.evolve(
                    inst=existing,
                    indexdef=indexdef,
                ),
            )

        else:
            self.__index_lane.store(
                key,
                self._index_def_stub(
                    schema=schema,
                    index=index,
                    indexdef=indexdef,
                ),
            )

        return indexdef

    # ....................... #

    async def get_index_def(self, *, index: str, schema: str | None = None) -> str:
        """Return the raw ``CREATE INDEX`` definition for an index.

        :param index: Index name.
        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :returns: The full index definition string.
        :raises exc.internal: If the index does not exist.
        """

        schema = self.__normalize_schema(schema)
        key = self._idx_key(schema, index)

        hit = self.__index_lane.lookup(key)

        if hit is not None:
            return hit.indexdef

        return await self.__inflight.run(
            ("pg_idxdef", *key),
            lambda: self._fetch_index_def_uncached(schema, index, key),
        )

    # ....................... #

    async def _fetch_index_info_uncached(
        self,
        schema: str,
        index: str,
        key: CacheKey,
    ) -> PostgresIndexInfo:
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
            raise exc.internal(f"Index not found: {schema}.{index}")

        amname = str(row.get("amname") or "")
        indexdef = str(row.get("indexdef") or "")
        expr = row.get("expr")
        expr_s = str(expr).strip() if expr is not None else None

        cols_raw: list[Any] = row.get("cols") or []
        columns = (
            tuple(str(x) for x in cols_raw)
            if isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
                cols_raw, (list, tuple)
            )
            else ()
        )

        has_tsvector_col = bool(row.get("has_tsvector_col") or False)

        engine: PostgresIndexEngine = "unknown"
        idx_l = indexdef.lower()

        if amname == "pgroonga":
            engine = "pgroonga"

        elif amname == "gin":
            if has_tsvector_col or ("to_tsvector(" in idx_l) or ("tsvector" in idx_l):
                engine = "fts"

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

        self.__index_lane.store(key, info)

        return info

    # ....................... #

    async def get_index_info(
        self,
        *,
        index: str,
        schema: str | None = None,
    ) -> PostgresIndexInfo:
        """Return full :class:`PostgresIndexInfo` for an index, classifying its engine.

        :param index: Index name.
        :param schema: Schema name (defaults to ``"public"`` when ``None``).
        :returns: Index metadata with classified engine.
        :raises exc.internal: If the index does not exist.
        """

        schema = self.__normalize_schema(schema)
        key = self._idx_key(schema, index)
        hit = self.__index_lane.lookup(key)

        if hit is not None and hit.amname:
            return hit

        return await self.__inflight.run(
            ("pg_idxinfo", *key),
            lambda: self._fetch_index_info_uncached(schema, index, key),
        )

    # ....................... #

    async def _fetch_relation_row_estimate_uncached(
        self,
        schema: str,
        relation: str,
        key: CacheKey,
    ) -> int:
        stmt = sql.SQL(
            """
            SELECT COALESCE(
                NULLIF(c.reltuples, -1)::bigint,
                s.n_live_tup,
                0
            ) AS estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s
                ON s.relid = c.oid
            WHERE n.nspname = {schema}
                AND c.relname = {relation}
            LIMIT 1
            """
        ).format(schema=sql.Placeholder(), relation=sql.Placeholder())

        val = await self.client.fetch_value(stmt, [schema, relation], default=0)
        estimate = max(int(val or 0), 0)
        self.__row_estimate_lane.store(key, estimate)

        return estimate

    # ....................... #

    async def estimate_relation_rows(
        self,
        *,
        schema: str | None,
        relation: str,
    ) -> int:
        """Return an approximate live row count for a relation (cached).

        Uses ``pg_class.reltuples`` when available, otherwise ``pg_stat_user_tables.n_live_tup``.
        """

        schema = self.__normalize_schema(schema)
        key = self._rel_key(schema, relation)
        hit = self.__row_estimate_lane.lookup(key)

        if hit is not None:
            return hit

        return await self.__inflight.run(
            ("pg_row_est", *key),
            lambda: self._fetch_relation_row_estimate_uncached(schema, relation, key),
        )

    # ....................... #

    @staticmethod
    def _where_cache_fingerprint(
        where_sql: sql.Composable,
        params: Sequence[Any],
    ) -> str:
        raw = f"{where_sql!s}:{params!r}"
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    # ....................... #

    @staticmethod
    def _plan_rows_from_explain_payload(payload: Any) -> int | None:
        if not isinstance(payload, list) or not payload:
            return None

        payload = cast(list[Any], payload)  # type: ignore[redundant-cast]
        root = payload[0]

        if not isinstance(root, dict):
            return None

        root = cast(JsonDict, root)
        plan = root.get("Plan")

        if not isinstance(plan, dict):
            return None

        plan = cast(JsonDict, plan)
        rows = plan.get("Plan Rows")

        if rows is None:
            return None

        try:
            return max(int(rows), 0)
        except (TypeError, ValueError):
            return None

    # ....................... #

    async def _fetch_filtered_row_estimate_uncached(
        self,
        *,
        schema: str,
        relation: str,
        where_sql: sql.Composable,
        params: Sequence[Any],
        cache_key: CacheKey,
    ) -> int:
        rel_ident = sql.Identifier(schema, relation)
        stmt = sql.SQL(
            "EXPLAIN (FORMAT JSON) SELECT 1 FROM {} WHERE {}",
        ).format(rel_ident, where_sql)

        raw = await self.client.fetch_value(stmt, list(params), default=None)
        estimate: int | None = None

        if raw is not None:
            if isinstance(raw, str):
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    payload = None
            else:
                payload = raw

            estimate = self._plan_rows_from_explain_payload(payload)

        if estimate is None or estimate <= 0:
            estimate = await self._fetch_relation_row_estimate_uncached(
                schema,
                relation,
                self._rel_key(schema, relation),
            )

        self.__filtered_row_estimate_lane.store(cache_key, estimate)
        return estimate

    # ....................... #

    async def estimate_filtered_rows(
        self,
        *,
        schema: str | None,
        relation: str,
        where_sql: sql.Composable,
        params: Sequence[Any],
    ) -> int:
        """Approximate rows matching ``where_sql`` via ``EXPLAIN (FORMAT JSON)``.

        Falls back to :meth:`estimate_relation_rows` when the planner returns no row estimate.
        """

        schema = self.__normalize_schema(schema)
        fp = self._where_cache_fingerprint(where_sql, params)
        key: CacheKey = (self._partition(), schema, f"{relation}:{fp}")
        hit = self.__filtered_row_estimate_lane.lookup(key)

        if hit is not None:
            return hit

        return await self.__inflight.run(
            ("pg_filt_est", *key),
            lambda: self._fetch_filtered_row_estimate_uncached(
                schema=schema,
                relation=relation,
                where_sql=where_sql,
                params=params,
                cache_key=key,
            ),
        )

    # ....................... #

    def invalidate_relation(self, *, schema: str | None, relation: str) -> None:
        """Evict cached relation kind and column types for a specific relation."""

        schema = self.__normalize_schema(schema)
        rk = self._rel_key(schema, relation)

        self.__relation_lane.invalidate(rk)
        self.__column_lane.invalidate(rk)
        self.__trigger_lane.invalidate(rk)
        self.__pk_lane.invalidate(rk)
        self.__unique_sets_lane.invalidate(rk)
        self.__row_estimate_lane.invalidate(rk)
        self.__filtered_row_estimate_lane.clear()

    # ....................... #

    def invalidate_index(self, *, schema: str | None, index: str) -> None:
        """Evict cached index info and definition for a specific index."""

        schema = self.__normalize_schema(schema)

        self.__index_lane.invalidate(self._idx_key(schema, index))

    # ....................... #

    def clear(self) -> None:
        """Clear all introspection caches."""

        self.__relation_lane.clear()
        self.__column_lane.clear()
        self.__trigger_lane.clear()
        self.__pk_lane.clear()
        self.__unique_sets_lane.clear()
        self.__index_lane.clear()
        self.__row_estimate_lane.clear()
        self.__filtered_row_estimate_lane.clear()
        self.__inflight.clear()
        self.__coalesce.clear_inflight()
