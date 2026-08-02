from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

from collections.abc import Mapping
from typing import Any

import aiohttp

from forze.base.exceptions import (
    CoreException,
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
    missing or unavailable *bucket* — a deployment fault, not a deleted object.
    Those are picked off by :func:`_bucket_404_is_configuration` ahead of the
    shared mapper; this predicate only says which 404s are caller misses.
    """

    request_info = getattr(error, "request_info", None)
    url = getattr(request_info, "url", None)
    path = getattr(url, "path", "")

    return "/o/" in path


def _bucket_404_is_configuration(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Map a bucket-level 404 to ``configuration``, deferring everything else.

    An arm rather than a widening of :func:`make_http_exception_mapper`, whose non-caller
    404 default is deliberately ``infrastructure`` for the backends that share it (a
    BigQuery dataset or a ClickHouse table missing is a different judgement call, and not
    this change's to make).

    The kind matters because the egress policy reads ``infrastructure`` as retryable, and
    an unprovisioned bucket never becomes provisioned by asking again: a saga or consumer
    retry loop would spin against it. ``configuration`` is non-retryable, withholds its
    details from clients exactly as ``infrastructure`` does, and still maps to HTTP 500.
    """

    if not isinstance(exc, aiohttp.ClientResponseError) or exc.status != 404:
        return None

    if _object_scoped_404(exc):
        return None

    _ = site

    return CoreException.configuration("GCS bucket not found.", details=details)


_gcs_eh = make_http_exception_mapper(
    label="GCS",
    response_error_type=aiohttp.ClientResponseError,
    http_status_message=_gcs_http_message,
    # An object-addressed 404 is caller-caused, not retryable downstream ill health.
    # Bucket-level 404s never reach this arm — see _bucket_404_is_configuration.
    missing_as_not_found=_object_scoped_404,
)
"""Normalize gcloud-aio / aiohttp GCS errors into the :class:`exc.internal` hierarchy."""

exc_interceptor = build_exc_interceptor("GCS", _bucket_404_is_configuration, _gcs_eh)
