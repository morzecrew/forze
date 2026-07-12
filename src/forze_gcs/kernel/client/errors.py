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

_gcs_eh = make_http_exception_mapper(
    label="GCS",
    response_error_type=aiohttp.ClientResponseError,
    http_status_message=_gcs_http_message,
    # GCS 404s here name objects the caller addresses by key: a miss is
    # caller-caused, not retryable downstream ill health. Bucket existence is
    # probed by the lifecycle ensure path before errors reach this mapper.
    missing_as_not_found=True,
)
"""Normalize gcloud-aio / aiohttp GCS errors into the :class:`exc.internal` hierarchy."""

exc_interceptor = build_exc_interceptor("GCS", _gcs_eh)
