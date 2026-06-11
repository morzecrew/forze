from forze_gcs._compat import require_gcs

require_gcs()

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

_shared_fallback = fallback_exception_mapper("GCS")

# ....................... #


def _gcs_http_message(status: int | None) -> str:
    if status is not None and status >= 500:
        return "GCS internal error."

    return f"GCS client error ({status})."


# ....................... #


def _gcs_fallback(
    exc: BaseException, site: str, details: Mapping[str, Any] | None
) -> CoreException:
    return _shared_fallback(exc, site=site, details=details)


# ....................... #

_gcs_eh = make_http_exception_mapper(
    label="GCS",
    response_error_type=aiohttp.ClientResponseError,
    http_status_message=_gcs_http_message,
    fallback=_gcs_fallback,
)
"""Normalize gcloud-aio / aiohttp GCS errors into the :class:`exc.internal` hierarchy."""

_gcs_chain = default_chain_exc_mapper.chain(_gcs_eh)
exc_interceptor = ExceptionInterceptor(mapper=_gcs_chain)
