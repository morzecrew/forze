from forze_http._compat import require_http

require_http()

# ....................... #

from collections.abc import Mapping
from typing import Any

import httpx

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)
from forze.base.exceptions import (
    exc as forze_exc,
)

# ----------------------- #


def _response_status(exc: BaseException) -> int | None:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code

    return None


# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _httpx_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize httpx errors into :class:`CoreException`."""

    _ = site

    match exc:
        case httpx.TimeoutException():
            return forze_exc.infrastructure(
                "HTTP request timed out.",
                details=details,
            )

        case httpx.ConnectError():
            return forze_exc.infrastructure(
                "HTTP connection failed.",
                details=details,
            )

        case httpx.HTTPStatusError() as http_err:
            status = _response_status(http_err)

            if status == 404:
                return forze_exc.not_found(
                    "HTTP resource not found.",
                    details=details,
                )

            if status in {401, 403}:
                return forze_exc.authentication(
                    "HTTP access denied.",
                    details=details,
                )

            if status == 429:
                return forze_exc.infrastructure(
                    "HTTP request throttled.",
                    details=details,
                )

            if status is not None and status >= 500:
                return forze_exc.infrastructure(
                    "HTTP upstream error.",
                    details=details,
                )

            return forze_exc.infrastructure(
                f"HTTP client error ({status}).",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("HTTP", _httpx_eh)
