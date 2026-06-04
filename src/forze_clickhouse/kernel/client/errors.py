from forze_clickhouse._compat import require_clickhouse

require_clickhouse()

# ....................... #

from collections.abc import Mapping
from typing import Any

import aiohttp

from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    default_chain_exc_mapper,
    make_http_exception_mapper,
)

# ----------------------- #


def _clickhouse_http_message(status: int | None) -> str:
    return f"ClickHouse request failed ({status})."


# ....................... #


def _clickhouse_fallback(
    exc: BaseException, site: str, details: Mapping[str, Any] | None
) -> CoreException:
    msg = str(exc).lower()

    if "authentication" in msg or "password" in msg:
        return CoreException.infrastructure(
            "ClickHouse access denied.",
            details=details,
        )

    return CoreException.infrastructure(
        f"ClickHouse error during {site}.",
        details=details,
    )


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
