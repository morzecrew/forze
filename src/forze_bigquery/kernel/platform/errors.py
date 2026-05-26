from forze_bigquery._compat import require_bigquery

require_bigquery()

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
def _bigquery_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize gcloud-aio / aiohttp BigQuery errors."""

    match exc:
        case CoreException():
            return exc

        case aiohttp.ClientResponseError() as cre:
            status = _response_status(cre)

            if status == 404:
                return CoreException.infrastructure(
                    "BigQuery resource not found.",
                    details=details,
                )

            if status in {401, 403}:
                return CoreException.infrastructure(
                    "BigQuery access denied.",
                    details=details,
                )

            if status == 429:
                return CoreException.infrastructure(
                    "BigQuery request throttled.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"BigQuery request failed ({status}).",
                details=details,
            )

        case _:
            return CoreException.infrastructure(
                f"BigQuery error during {site}.",
                details=details,
            )


# ....................... #

_bq_chain = default_chain_exc_mapper.chain(_bigquery_eh)
exc_interceptor = ExceptionInterceptor(mapper=_bq_chain)
