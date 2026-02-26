from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Optional, TypeAlias, final

import attrs
from psycopg import sql

from forze.base.errors import CoreError

from ..platform import PostgresClient
from .utils import normalize_pg_type

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresType:
    base: str
    is_array: bool
    not_null: bool


PostgresColumnTypes: TypeAlias = dict[str, PostgresType]
PostgresColumnCache: TypeAlias = dict[tuple[Optional[str], str], PostgresColumnTypes]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class PostgresTypesProvider:
    client: PostgresClient = attrs.field(on_setattr=attrs.setters.frozen)
    _cache: PostgresColumnCache = attrs.field(factory=PostgresColumnCache, init=False)

    # ....................... #

    async def get(self, *, schema: Optional[str], table: str) -> PostgresColumnTypes:
        key = (schema, table)

        if key in self._cache:
            return self._cache[key]

        # Notes:
        # - format_type(atttypid, atttypmod) gives human-friendly type strings.
        # - for arrays, we derive base element type via pg_type.typelem and format_type.
        # - attnotnull is reliable for tables; for views it’s generally false (unless
        #   view column is marked NOT NULL via domain/expr constraints—not common).
        stmt = sql.SQL(
            """
            WITH rel AS (
              SELECT c.oid
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = {schema}
                AND c.relname = {rel}
              LIMIT 1
            )
            SELECT
              a.attnum,
              a.attname AS column_name,
              -- element type for arrays; NULL for non-arrays
              CASE
                WHEN t.typelem <> 0 THEN format_type(t.typelem, NULL)
                ELSE NULL
              END AS array_elem_type,
              -- full type (may be 'uuid[]', 'timestamp with time zone', etc.)
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
        ).format(
            schema=sql.Placeholder(),
            rel=sql.Placeholder(),
        )

        rows = await self.client.fetch_all(stmt, [schema, table], row_factory="dict")

        if not rows:
            raise CoreError(
                f"Таблица не найдена или не имеет столбцов: {schema}.{table}"
            )

        out: dict[str, PostgresType] = {}

        for r in rows:
            col = r["column_name"]
            is_array = bool(r["is_array"])
            not_null = bool(r["not_null"])

            if is_array:
                elem = r["array_elem_type"]
                if not elem:
                    # Shouldn't happen, but keep safe.
                    base = normalize_pg_type(str(r["full_type"]).rstrip("[]"))

                else:
                    base = normalize_pg_type(str(elem))

            else:
                base = normalize_pg_type(str(r["full_type"]))

            out[col] = PostgresType(base=base, is_array=is_array, not_null=not_null)

        self._cache[key] = out

        return out

    # ....................... #

    def invalidate(self, *, schema: Optional[str], relation: str) -> None:
        """Drop cache for a specific relation."""

        self._cache.pop((schema, relation), None)

    # ....................... #

    def clear(self) -> None:
        """Drop the whole cache."""

        self._cache.clear()
