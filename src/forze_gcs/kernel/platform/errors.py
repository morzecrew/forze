from forze_gcs._compat import require_gcs

require_gcs()

# ....................... #

from functools import partial
from typing import Any

import aiohttp

from forze.base.errors import CoreError, InfrastructureError, error_handler, handled

# ----------------------- #


def _response_status(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None)

    if status is not None:
        return int(status)

    code = getattr(exc, "code", None)

    if code is not None:
        return int(code)

    return None


@error_handler
def _gcs_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Normalize gcloud-aio / aiohttp GCS errors into :class:`CoreError` hierarchy."""

    match e:
        case CoreError():
            return e

        case aiohttp.ClientResponseError() as cre:
            status = _response_status(cre)

            if status == 404:
                return InfrastructureError("GCS resource not found.")

            if status in {401, 403}:
                return InfrastructureError("GCS access denied.")

            if status == 429:
                return InfrastructureError("GCS request throttled.")

            if status is not None and status >= 500:
                return InfrastructureError("GCS internal error.")

            return InfrastructureError(f"GCS client error ({status}).")

        case _:
            return InfrastructureError(
                f"An error occurred while executing GCS operation {op}: {e}"
            )


# ----------------------- #

gcs_handled = partial(handled, _gcs_eh)
