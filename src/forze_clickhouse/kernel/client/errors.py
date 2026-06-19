from forze_clickhouse._compat import require_clickhouse

require_clickhouse()

# ....................... #

import re
from collections.abc import Mapping
from typing import Any

import aiohttp

from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    default_chain_exc_mapper,
    fallback_exception_mapper,
    make_http_exception_mapper,
)

# ----------------------- #

_shared_fallback = fallback_exception_mapper("ClickHouse")

# ClickHouse server errors carry a stable numeric code: ``Code: NNN. DB::...``.
# 516 = AUTHENTICATION_FAILED, 497 = ACCESS_DENIED.
_CLICKHOUSE_CODE_RE = re.compile(r"Code:\s*(\d+)")
_CLICKHOUSE_ACCESS_CODES = frozenset({"497", "516"})

# ....................... #


def _clickhouse_http_message(status: int | None) -> str:
    return f"ClickHouse request failed ({status})."


# ....................... #


def _clickhouse_fallback(
    exc: BaseException, site: str, details: Mapping[str, Any] | None
) -> CoreException:
    # Classify by the numeric server error code rather than matching English
    # words in the message (which would misfire on a query/literal that merely
    # contains "password").
    match = _CLICKHOUSE_CODE_RE.search(str(exc))

    if match is not None and match.group(1) in _CLICKHOUSE_ACCESS_CODES:
        return CoreException.infrastructure(
            "ClickHouse access denied.",
            details=details,
        )

    return _shared_fallback(exc, site=site, details=details)


# ....................... #

_clickhouse_eh = make_http_exception_mapper(
    label="ClickHouse",
    response_error_type=aiohttp.ClientResponseError,
    http_status_message=_clickhouse_http_message,
    fallback=_clickhouse_fallback,
)
"""Normalize clickhouse-connect / aiohttp errors into the :class:`exc.internal` hierarchy."""

_clickhouse_chain = default_chain_exc_mapper.chain(_clickhouse_eh)
exc_interceptor = ExceptionInterceptor(mapper=_clickhouse_chain)
