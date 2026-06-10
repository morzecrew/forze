from forze_bigquery._compat import require_bigquery

require_bigquery()

# ....................... #

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

_shared_fallback = fallback_exception_mapper("BigQuery")

# ....................... #


def _bigquery_http_message(status: int | None) -> str:
    return f"BigQuery request failed ({status})."


# ....................... #


def _bigquery_fallback(
    exc: BaseException, site: str, details: Mapping[str, Any] | None
) -> CoreException:
    return _shared_fallback(exc, site=site, details=details)


# ....................... #

_bigquery_eh = make_http_exception_mapper(
    label="BigQuery",
    response_error_type=aiohttp.ClientResponseError,
    http_status_message=_bigquery_http_message,
    fallback=_bigquery_fallback,
)
"""Normalize gcloud-aio / aiohttp BigQuery errors into the :class:`exc.internal` hierarchy."""

_bq_chain = default_chain_exc_mapper.chain(_bigquery_eh)
exc_interceptor = ExceptionInterceptor(mapper=_bq_chain)
