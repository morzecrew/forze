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
    fallback: Callable[[BaseException, str, Mapping[str, Any] | None], CoreException],
) -> ExceptionMapper:
    """Build an :class:`ExceptionMapper` for aiohttp-style HTTP backends.

    Passes :class:`CoreException` through unchanged. For *response_error_type*
    instances, maps ``404``/``401``/``403``/``429`` to standard ``{label} ...``
    infrastructure errors and defers other statuses to *http_status_message*.
    Non-HTTP errors are handed to *fallback*.

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

        return fallback(exc, site, details)

    return _map
