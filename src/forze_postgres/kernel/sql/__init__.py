"""Psycopg SQL helpers: query rendering, type casts, keyset pagination, conflict targets."""

from forze.application.contracts.querying import (
    decode_keyset_v1,
    encode_keyset_v1,
    normalize_sorts_with_id,
    row_value_for_sort_key,
)

from .analytics import apply_limit_offset, build_count_sql, parameters_from_model
from .conflict_target import resolve_write_conflict_target
from .query import PsycopgQueryRenderer
from .query.nested import sort_key_expr
from .query.utils import PsycopgPositionalBinder
from .seek import (
    Nav,
    build_order_by_sql,
    build_ranked_cursor_order_by_sql,
    build_seek_condition,
)
from .type_cast import (
    assignment_from_values_column,
    cast_sql_for_column_type,
)

# ----------------------- #

__all__ = [
    "apply_limit_offset",
    "build_count_sql",
    "parameters_from_model",
    "Nav",
    "PsycopgPositionalBinder",
    "PsycopgQueryRenderer",
    "assignment_from_values_column",
    "build_order_by_sql",
    "build_ranked_cursor_order_by_sql",
    "build_seek_condition",
    "cast_sql_for_column_type",
    "decode_keyset_v1",
    "encode_keyset_v1",
    "normalize_sorts_with_id",
    "resolve_write_conflict_target",
    "row_value_for_sort_key",
    "sort_key_expr",
]
