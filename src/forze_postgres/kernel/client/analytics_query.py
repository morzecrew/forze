"""SQL helpers for Postgres analytics queries."""

from __future__ import annotations

from pydantic import BaseModel

# ----------------------- #


def parameters_from_model(params: BaseModel) -> dict[str, object]:
    """Build psycopg named parameters from a Pydantic model."""

    return params.model_dump()


# ....................... #
#! TODO: move to SQL folder


def apply_limit_offset(
    sql: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """Append ``LIMIT`` / ``OFFSET`` clauses to *sql*."""

    stripped = sql.strip().rstrip(";")
    parts: list[str] = []

    if limit is not None:
        parts.append(f"LIMIT {int(limit)}")

    if offset is not None:
        parts.append(f"OFFSET {int(offset)}")

    if not parts:
        return stripped

    return f"{stripped} {' '.join(parts)}"


# ....................... #
#! TODO: move to SQL folder


def build_count_sql(inner_sql: str) -> str:
    """Wrap *inner_sql* in ``SELECT COUNT(*)`` for total row counts."""

    stripped = inner_sql.strip().rstrip(";")
    return f"SELECT COUNT(*) AS forze_cnt FROM ({stripped}) AS forze_analytics_subq"  # nosec B608
