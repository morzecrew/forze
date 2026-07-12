from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

import aiohttp

from forze.base.exceptions import (
    build_exc_interceptor,
    make_http_exception_mapper,
)

# ----------------------- #


def _gcs_http_message(status: int | None) -> str:
    if status is not None and status >= 500:
        return "GCS internal error."

    return f"GCS client error ({status})."


# ....................... #

def _object_scoped_404(error: BaseException) -> bool:
    """Only an object-addressed 404 is a caller miss.

    GCS object URLs carry ``/o/<name>`` (read/head/delete); bucket-level and
    upload URLs (``/b/<bucket>``, ``…/o`` with no object segment) 404 on a
    missing or unavailable *bucket* — a deployment fault that must stay
    ``infrastructure``, not read as a deleted object.
    """

    request_info = getattr(error, "request_info", None)
    url = getattr(request_info, "url", None)
    path = getattr(url, "path", "")

    return "/o/" in path


_gcs_eh = make_http_exception_mapper(
    label="GCS",
    response_error_type=aiohttp.ClientResponseError,
    http_status_message=_gcs_http_message,
    # An object-addressed 404 is caller-caused, not retryable downstream ill
    # health; bucket-level 404s keep the infrastructure default.
    missing_as_not_found=_object_scoped_404,
)
"""Normalize gcloud-aio / aiohttp GCS errors into the :class:`exc.internal` hierarchy."""

exc_interceptor = build_exc_interceptor("GCS", _gcs_eh)
