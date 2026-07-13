"""Shared catch-all exception mapper for integration backends."""

from collections.abc import Mapping
from typing import Any, Protocol

from .model import CoreException

# ----------------------- #


class FallbackExceptionMapper(Protocol):
    """Terminal :class:`~.protocols.ExceptionMapper` that always maps."""

    def __call__(
        self,
        exc: BaseException,
        *,
        site: str,
        details: Mapping[str, Any] | None = None,
    ) -> CoreException: ...


# ....................... #


def fallback_exception_mapper(backend: str) -> FallbackExceptionMapper:
    """Build the standard catch-all arm for *backend* error mappers.

    The returned mapper keeps the summary static: raw driver exception text
    may carry internal data. The stringified error goes into
    ``details["error"]``, which egress suppresses and the scrubber sanitizes.

    :param backend: Human-readable backend label (e.g. ``"Postgres"``).
    :returns: A terminal mapper producing an ``infrastructure``
        :class:`CoreException` for any exception (passing
        :class:`CoreException` instances through unchanged).
    """

    def _fallback(
        exc: BaseException,
        *,
        site: str,
        details: Mapping[str, Any] | None = None,
    ) -> CoreException:
        if isinstance(exc, CoreException):
            return exc

        return CoreException.infrastructure(
            f"An error occurred during {backend} operation {site}.",
            details={**(details or {}), "error": str(exc)},
        )

    return _fallback
