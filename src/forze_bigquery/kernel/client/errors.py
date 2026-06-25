from forze_bigquery._compat import require_bigquery

require_bigquery()

# ....................... #

import aiohttp

from forze.base.exceptions import (
    build_exc_interceptor,
    make_http_exception_mapper,
)

# ----------------------- #


def _bigquery_http_message(status: int | None) -> str:
    return f"BigQuery request failed ({status})."


# ....................... #

_bigquery_eh = make_http_exception_mapper(
    label="BigQuery",
    response_error_type=aiohttp.ClientResponseError,
    http_status_message=_bigquery_http_message,
)
"""Normalize gcloud-aio / aiohttp BigQuery errors into the :class:`exc.internal` hierarchy."""

exc_interceptor = build_exc_interceptor("BigQuery", _bigquery_eh)
