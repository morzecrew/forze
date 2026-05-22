"""Postgres column type casts for bulk writes and JSON-path filters."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from psycopg import sql

from .introspect import PostgresType

# ----------------------- #

_TEXT_LIKE = frozenset({"text", "varchar", "char", "citext"})


def _scalar_cast_name(base: str) -> sql.Composable | None:
    """Return a cast target for a scalar type name, or ``None`` to use fallback / no cast."""

    if base in _TEXT_LIKE:
        return None

    match base:
        case "uuid":
            return sql.SQL("uuid")
        case "int2" | "int4" | "int8":
            return sql.SQL(base)
        case "float4" | "float8" | "numeric":
            return sql.SQL(base)
        case "bool":
            return sql.SQL("boolean")
        case "date":
            return sql.SQL("date")
        case "timestamptz" | "timestamp":
            return sql.SQL(base)
        case "json" | "jsonb":
            return sql.SQL(base)
        case _:
            return None


def cast_sql_for_column_type(pg: PostgresType) -> sql.Composable | None:
    """SQL type name for ``CAST(... AS ...)`` / ``::`` from introspected column metadata.

    :returns: Composable type name, or ``None`` when no cast is needed (text-like columns).
    """

    if pg.is_array:
        inner = _scalar_cast_name(pg.base)

        if inner is not None:
            return sql.Composed([inner, sql.SQL("[]")])

        if pg.base in _TEXT_LIKE:
            return None

        return sql.Composed([_catalog_type_sql(pg.base), sql.SQL("[]")])

    if pg.base in _TEXT_LIKE:
        return None

    cast = _scalar_cast_name(pg.base)

    if cast is not None:
        return cast

    return _catalog_type_sql(pg.base)


def _catalog_type_sql(name: str) -> sql.Composable:
    """Wrap a ``pg_catalog`` type name as SQL (trusted, not user input)."""

    return sql.SQL(name)  # type: ignore[arg-type]


def assignment_from_values_column(
    col: str,
    pg: PostgresType | None,
    *,
    values_alias: str = "v",
) -> sql.Composable:
    """Build ``SET`` assignment ``col = v.col`` with optional cast from ``VALUES`` inference."""

    col_ident = sql.Identifier(col)
    v_col = sql.Identifier(values_alias, col)
    cast = cast_sql_for_column_type(pg) if pg is not None else None

    if cast is None:
        return sql.SQL("{} = {}").format(col_ident, v_col)

    return sql.SQL("{} = {}::{}").format(col_ident, v_col, cast)
