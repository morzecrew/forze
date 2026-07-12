"""Factory for HTTP-style backend exception mappers (aiohttp-based: GCS, BigQuery, ClickHouse)."""

from collections.abc import Callable, Mapping
from typing import Any

from .model import CoreException
from .protocols import ExceptionMapper

# ----------------------- #


def response_status(exc: BaseException) -> int | None:
    """Extract an HTTP status from an exception's ``status`` or ``code`` attribute."""

    status = getattr(exc, "status", None)

    if status is not None:
        return int(status)

    code = getattr(exc, "code", None)

    if code is not None:
        return int(code)

    return None


# ....................... #


def make_http_exception_mapper(
    *,
    label: str,
    response_error_type: type[BaseException],
    http_status_message: Callable[[int | None], str],
    fallback: ExceptionMapper | None = None,
    missing_as_not_found: bool = False,
) -> ExceptionMapper:
    """Build an :class:`ExceptionMapper` for aiohttp-style HTTP backends.

    Passes :class:`CoreException` through unchanged. For *response_error_type*
    instances, maps ``404``/``401``/``403``/``429`` to standard ``{label} ...``
    infrastructure errors and defers other statuses to *http_status_message*.
    A non-HTTP error is handed to *fallback*, or — when *fallback* is ``None`` —
    returned as ``None`` so an enclosing chain (e.g. :func:`build_exc_interceptor`)
    supplies the terminal fallback.

    *missing_as_not_found* flips a ``404`` to caller-caused ``not_found`` — for a
    backend whose 404s name resources the caller addresses by key (object storage),
    where a miss must not be retried or counted against downstream health. Backends
    whose 404s indicate deployment faults (a missing table or dataset) keep the
    ``infrastructure`` default.

    ``aiohttp`` is not imported here; callers pass ``aiohttp.ClientResponseError``
    as *response_error_type* so core keeps no dependency on it.
    """

    def _map(
        exc: BaseException,
        *,
        site: str,
        details: Mapping[str, Any] | None = None,
    ) -> CoreException | None:
        if isinstance(exc, CoreException):
            return exc

        if isinstance(exc, response_error_type):
            status = response_status(exc)

            if status == 404:
                if missing_as_not_found:
                    return CoreException.not_found(
                        f"{label} resource not found.", details=details
                    )

                return CoreException.infrastructure(
                    f"{label} resource not found.", details=details
                )

            if status in {401, 403}:
                return CoreException.infrastructure(
                    f"{label} access denied.", details=details
                )

            if status == 429:
                return CoreException.infrastructure(
                    f"{label} request throttled.", details=details
                )

            return CoreException.infrastructure(
                http_status_message(status), details=details
            )

        if fallback is not None:
            return fallback(exc, site=site, details=details)

        return None

    return _map
