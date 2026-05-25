from forze_bigquery._compat import require_bigquery

require_bigquery()

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
def _bigquery_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Normalize gcloud-aio / aiohttp BigQuery errors."""

    match e:
        case CoreError():
            return e

        case aiohttp.ClientResponseError() as cre:
            status = _response_status(cre)

            if status == 404:
                return InfrastructureError("BigQuery resource not found.")

            if status in {401, 403}:
                return InfrastructureError("BigQuery access denied.")

            if status == 429:
                return InfrastructureError("BigQuery request throttled.")

            return InfrastructureError(f"BigQuery request failed ({status}).")

        case _:
            return InfrastructureError(f"BigQuery error during {op}.")


bigquery_handled = partial(handled, _bigquery_eh)
