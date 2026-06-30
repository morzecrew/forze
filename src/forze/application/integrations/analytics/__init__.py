"""Shared warehouse analytics adapter helpers."""

from .adapter_common import (
    TENANT_PARAM,
    assert_tenant_param_referenced,
    bind_tenant_param,
    decrypt_and_shape_rows,
    dry_run_enabled,
    dry_run_offset_page,
    encode_keyset_cursor_next,
    encode_offset_cursor_next_prev,
    execute_analytics_offset_page,
    merge_forze_after_params,
    pagination_window,
    parse_analytics_cursor_limit,
    parse_count_row,
    parse_keyset_cursor_after,
    parse_offset_cursor_after,
    shape_rows,
    stream_shaped_chunks,
    timeout_seconds,
    validated_params,
)
from .encryption import encode_ingest_payloads, resolve_analytics_codecs_spec
from .port import AnalyticsQueryPortMixin
from .sql import (
    COUNT_COLUMN,
    apply_limit_offset,
    build_count_sql,
    parameters_from_model,
)

# ----------------------- #

__all__ = [
    "AnalyticsQueryPortMixin",
    "COUNT_COLUMN",
    "TENANT_PARAM",
    "apply_limit_offset",
    "assert_tenant_param_referenced",
    "bind_tenant_param",
    "build_count_sql",
    "decrypt_and_shape_rows",
    "encode_ingest_payloads",
    "parameters_from_model",
    "resolve_analytics_codecs_spec",
    "dry_run_enabled",
    "dry_run_offset_page",
    "encode_keyset_cursor_next",
    "encode_offset_cursor_next_prev",
    "execute_analytics_offset_page",
    "merge_forze_after_params",
    "pagination_window",
    "parse_analytics_cursor_limit",
    "parse_count_row",
    "parse_keyset_cursor_after",
    "parse_offset_cursor_after",
    "shape_rows",
    "stream_shaped_chunks",
    "timeout_seconds",
    "validated_params",
]
