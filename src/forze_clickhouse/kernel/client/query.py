"""SQL helpers for ClickHouse analytics queries.

Thin wrappers over the shared analytics SQL builders in
:mod:`forze.application.integrations.analytics.sql`.
"""

from __future__ import annotations

from forze.application.integrations.analytics.sql import (
    apply_limit_offset,
    parameters_from_model,
)
from forze.application.integrations.analytics.sql import (
    build_count_sql as _build_count_sql,
)

# ----------------------- #

__all__ = [
    "apply_limit_offset",
    "build_count_sql",
    "parameters_from_model",
]

# ....................... #


def build_count_sql(inner_sql: str) -> str:
    """Wrap *inner_sql* in ``SELECT count()`` for total row counts."""

    return _build_count_sql(inner_sql, count_expr="count()")
