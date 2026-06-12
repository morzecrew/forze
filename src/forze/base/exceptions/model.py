from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Mapping, Protocol, Self

import attrs

from .enrichment import enrich as enrich_fn

if TYPE_CHECKING:
    from ..primitives import JsonDict
    from .enrichment import CallsiteFrame

# ----------------------- #


class ExceptionKind(StrEnum):
    """The kind of an exception."""

    INTERNAL = "internal"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    CONCURRENCY = "concurrency"
    VALIDATION = "validation"
    DOMAIN = "domain"
    PRECONDITION = "precondition"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    CONFIGURATION = "configuration"
    INFRASTRUCTURE = "infrastructure"

    THROTTLED = "throttled"
    """The call was rejected by a rate limit (no capacity right now).

    Transient by definition — capacity refills over time — so the kind is
    **retryable**: composing a rate-limited call with a
    :class:`~forze.application.contracts.resilience.RetryStrategy` that
    includes ``THROTTLED`` in ``retry_on`` turns reject-immediately into
    wait-with-backoff. Maps to HTTP 429 at the FastAPI edge.
    """


# ....................... #


class _CoreExceptionOfKind(Protocol):
    """Protocol for building :class:`CoreException` of a given kind."""

    def __call__(
        self,
        summary: str,
        *,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> CoreException: ...


# ....................... #


@attrs.define(slots=True)
class _exc_of_kind:
    """Descriptor for building :class:`CoreException` of a given kind."""

    kind: ExceptionKind

    # ....................... #

    def __get__(
        self,
        obj: object | None,
        owner: type[CoreException],
    ) -> _CoreExceptionOfKind:
        def factory(
            summary: str,
            *,
            code: str | None = None,
            details: Mapping[str, Any] | None = None,
        ) -> CoreException:
            return owner.of(self.kind, summary, code=code, details=details)

        factory.__name__ = self.kind.value
        factory.__doc__ = f"Build a ``{self.kind.value}`` :class:`CoreException`."
        return factory


# ....................... #


def _normalize_code(code: str) -> str:
    return str(code).lower().strip()


# ....................... #


@attrs.define(slots=True, eq=False)
class CoreException(Exception):
    """Base exception class for all forze exceptions."""

    kind: ExceptionKind
    """The kind of the exception."""

    summary: str
    """The summary of the exception."""

    code: str = attrs.field(converter=_normalize_code, kw_only=True)
    """The code of the exception."""

    details: Mapping[str, Any] | None = attrs.field(default=None, kw_only=True)
    """The details of the exception."""

    # ....................... #

    def __str__(self) -> str:
        return f"{str(self.kind).capitalize()} exception occurred ({self.code}): {self.summary}"

    # ....................... #

    @classmethod
    def of(
        cls,
        kind: ExceptionKind,
        summary: str,
        *,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> Self:
        """Build a :class:`CoreException` of the given kind."""

        return cls(
            kind=kind,
            summary=summary,
            code=code or f"core.{kind.value}",
            details=details,
        )

    # ....................... #

    internal = _exc_of_kind(ExceptionKind.INTERNAL)
    """Build an internal exception."""

    not_found = _exc_of_kind(ExceptionKind.NOT_FOUND)
    """Build a not found exception."""

    conflict = _exc_of_kind(ExceptionKind.CONFLICT)
    """Build a conflict exception."""

    concurrency = _exc_of_kind(ExceptionKind.CONCURRENCY)
    """Build a concurrency exception."""

    validation = _exc_of_kind(ExceptionKind.VALIDATION)
    """Build a validation exception."""

    domain = _exc_of_kind(ExceptionKind.DOMAIN)
    """Build a domain exception."""

    precondition = _exc_of_kind(ExceptionKind.PRECONDITION)
    """Build a precondition exception."""

    authentication = _exc_of_kind(ExceptionKind.AUTHENTICATION)
    """Build an authentication exception."""

    authorization = _exc_of_kind(ExceptionKind.AUTHORIZATION)
    """Build an authorization exception."""

    configuration = _exc_of_kind(ExceptionKind.CONFIGURATION)
    """Build a configuration exception."""

    infrastructure = _exc_of_kind(ExceptionKind.INFRASTRUCTURE)
    """Build an infrastructure exception."""

    throttled = _exc_of_kind(ExceptionKind.THROTTLED)
    """Build a throttled (rate-limited) exception."""

    # ....................... #

    def enrich(
        self,
        *,
        callsite: "CallsiteFrame | JsonDict | None" = None,
        resource: "JsonDict | None" = None,
        cause: "JsonDict | None" = None,
        **semantic: Any,
    ) -> Self:
        """Enrich the exception with additional details."""

        return enrich_fn(
            self,
            callsite=callsite,
            resource=resource,
            cause=cause,
            **semantic,
        )
