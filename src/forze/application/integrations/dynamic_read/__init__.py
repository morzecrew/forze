"""Shared governance shell and error taxonomy for the dynamic-read plane."""

from .adapter import (
    POSTGRES_TENANT_PLACEHOLDER,
    DynamicReadAdapter,
    DynamicReadRequest,
)
from .errors import (
    DYNAMIC_READ_CODES,
    MULTI_STATEMENT_CODE,
    NAMESPACE_MISSING_CODE,
    PERMISSION_DENIED_CODE,
    ROLE_UNAVAILABLE_CODE,
    ROW_CAP_EXCEEDED_CODE,
    STATEMENT_INVALID_CODE,
    STATEMENT_TOO_LARGE_CODE,
    TIMEOUT_CODE,
    WRITE_REFUSED_CODE,
    multi_statement,
    namespace_missing,
    permission_denied,
    role_unavailable,
    row_cap_exceeded,
    statement_invalid,
    statement_too_large,
    timed_out,
    write_refused,
)

# ----------------------- #

__all__ = [
    "DYNAMIC_READ_CODES",
    "MULTI_STATEMENT_CODE",
    "NAMESPACE_MISSING_CODE",
    "PERMISSION_DENIED_CODE",
    "POSTGRES_TENANT_PLACEHOLDER",
    "ROLE_UNAVAILABLE_CODE",
    "ROW_CAP_EXCEEDED_CODE",
    "STATEMENT_INVALID_CODE",
    "STATEMENT_TOO_LARGE_CODE",
    "TIMEOUT_CODE",
    "WRITE_REFUSED_CODE",
    "DynamicReadAdapter",
    "DynamicReadRequest",
    "multi_statement",
    "namespace_missing",
    "permission_denied",
    "role_unavailable",
    "row_cap_exceeded",
    "statement_invalid",
    "statement_too_large",
    "timed_out",
    "write_refused",
]
