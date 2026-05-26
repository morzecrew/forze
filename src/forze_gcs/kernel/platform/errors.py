from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

from typing import Any, Mapping

import aiohttp

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    ExceptionMapper,
    default_chain_exc_mapper,
)

# ----------------------- #


def _response_status(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None)

    if status is not None:
        return int(status)

    code = getattr(exc, "code", None)

    if code is not None:
        return int(code)

    return None


# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _gcs_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize gcloud-aio / aiohttp GCS errors into :class:`exc.internal` hierarchy."""

    match exc:
        case CoreException():
            return exc

        case aiohttp.ClientResponseError() as cre:
            status = _response_status(cre)

            if status == 404:
                return CoreException.infrastructure(
                    "GCS resource not found.",
                    details=details,
                )

            if status in {401, 403}:
                return CoreException.infrastructure(
                    "GCS access denied.", details=details
                )

            if status == 429:
                return CoreException.infrastructure(
                    "GCS request throttled.", details=details
                )

            if status is not None and status >= 500:
                return CoreException.infrastructure(
                    "GCS internal error.", details=details
                )

            return CoreException.infrastructure(
                f"GCS client error ({status}).", details=details
            )

        case _:
            return CoreException.infrastructure(
                f"An error occurred while executing GCS operation {site}: {exc}",
                details=details,
            )


# ....................... #

_gcs_chain = default_chain_exc_mapper.chain(_gcs_eh)
exc_interceptor = ExceptionInterceptor(mapper=_gcs_chain)
