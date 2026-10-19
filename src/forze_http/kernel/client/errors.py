from forze_http._compat import require_http

require_http()

# ....................... #

from collections.abc import Mapping
from datetime import UTC
from email.utils import parsedate_to_datetime
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
from forze.base.primitives import utcnow

# ----------------------- #


def _response_status(exc: BaseException) -> int | None:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code

    return None


_MAX_RETRY_AFTER = 3600.0
"""The longest ``Retry-After`` carried, in seconds: past it the value is noise, not a schedule."""


def _retry_after(response: httpx.Response) -> float | None:
    """Seconds the upstream asked to wait (``Retry-After`` as seconds or an HTTP date),
    within ``[0, 3600]``, or ``None`` when it sent none that parses."""

    value = response.headers.get("retry-after", "").strip()

    # ASCII only: `str.isdigit` also accepts digits such as "²" that `float` refuses.
    if value.isascii() and value.isdigit():
        seconds = float(value)

    else:
        try:
            when = parsedate_to_datetime(value)

        except (TypeError, ValueError, OverflowError):
            return None

        # A date with no zone (``-0000``) parses naive; HTTP dates are UTC.
        when = when if when.tzinfo is not None else when.replace(tzinfo=UTC)
        seconds = (when - utcnow()).total_seconds()

    return min(max(0.0, seconds), _MAX_RETRY_AFTER)


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

            # The upstream refused this service's credential, not the caller's: an operator
            # fixes it, and it must not reach the caller as their own 401.
            if status in {401, 403}:
                return forze_exc.infrastructure(
                    "HTTP access denied.",
                    details=details,
                )

            if status == 429:
                retry_after = _retry_after(http_err.response)

                return forze_exc.throttled(
                    "HTTP request throttled.",
                    details=details
                    if retry_after is None
                    else {**(details or {}), "retry_after": retry_after},
                )

            if status == 408 or (status is not None and status >= 500):
                return forze_exc.infrastructure(
                    "HTTP upstream error.",
                    details=details,
                )

            # A redirect is not followed: the service's address is stale, not the caller's.
            if status is not None and 300 <= status < 400:
                return forze_exc.internal(
                    f"HTTP redirect not followed ({status}).",
                    details=details,
                )

            # The upstream rejected the request this service built: not the caller's fault,
            # and asking again fails the same way.
            return forze_exc.internal(
                f"HTTP client error ({status}).",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("HTTP", _httpx_eh)
