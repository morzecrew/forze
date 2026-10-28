from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, Self

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

    TIMEOUT = "timeout"
    """The invocation deadline elapsed before the call completed.

    **Not retryable**: within the same invocation the time budget is already
    spent, so an in-process retry would only burn what little remains. A fresh
    invocation carries a fresh deadline — retrying is the *caller's* decision.
    Distinct from a per-attempt resilience timeout, which raises
    ``INFRASTRUCTURE`` precisely so retry strategies can take another attempt.
    Maps to HTTP 504 at the FastAPI edge.
    """

    UNAVAILABLE = "unavailable"
    """The call works, but is switched off right now — by configuration, a
    maintenance window, or a dependency an operator turned off on purpose.

    **Retryable**: the state is temporary by definition. Maps to HTTP 503 at
    the FastAPI edge, yet it reports a state an operator chose, not a fault, so
    its summary and code reach the client and it is not logged as a server
    error. A backing system that *failed* is ``INFRASTRUCTURE``.
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
        resource_type: str | None = None,
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
            resource_type: str | None = None,
        ) -> CoreException:
            return owner.of(
                self.kind,
                summary,
                code=code,
                details=details,
                resource_type=resource_type,
            )

        factory.__name__ = self.kind.value
        factory.__doc__ = f"Build a ``{self.kind.value}`` :class:`CoreException`."
        return factory


# ....................... #


def _normalize_code(code: str) -> str:
    return str(code).lower().strip()


# ....................... #


def _rebuild[E: CoreException](
    cls: type[E],
    fields: Mapping[str, Any],
    state: Mapping[str, Any],
) -> E:
    """Rebuild an exception from :meth:`CoreException.__reduce__`."""

    rebuilt = cls(**fields)
    rebuilt.__dict__.update(state)
    return rebuilt


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

    resource_type: str | None = attrs.field(default=None, kw_only=True)
    """The resource type this error is about (a document spec name), or :obj:`None`.

    Read by :func:`~forze.base.exceptions.error_envelope` under a non-disclosing
    :class:`~forze.base.exceptions.DenialPosture`: an ``authorization`` or ``not_found``
    error about a covered type renders as one canonical not-found. Set by the authz hook
    for a denial about a resource and by the document adapters for a missing row."""

    expose_details: bool = attrs.field(default=True, kw_only=True)
    """Whether :func:`~forze.base.exceptions.error_envelope` may show :attr:`details` to a
    client at all; the kind's egress policy decides beyond that. ``False`` on every error an
    exception mapper translated from a backend's: its details are the failed call's own
    context, kept for the server."""

    # ....................... #

    def __str__(self) -> str:
        return f"{str(self.kind).capitalize()} exception occurred ({self.code}): {self.summary}"

    # ....................... #

    def __reduce__(self) -> tuple[Any, ...]:
        # BaseException rebuilds from ``args``: positional, and frozen at construction, so a
        # field changed in place (``expose_details``) would revert. Rebuild from the fields.
        fields = {f.alias: getattr(self, f.name) for f in attrs.fields(type(self)) if f.init}
        return _rebuild, (type(self), fields, self.__dict__)

    # ....................... #

    @classmethod
    def of(
        cls,
        kind: ExceptionKind,
        summary: str,
        *,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
        resource_type: str | None = None,
    ) -> Self:
        """Build a :class:`CoreException` of the given kind."""

        return cls(
            kind=kind,
            summary=summary,
            code=code or f"core.{kind.value}",
            details=details,
            resource_type=resource_type,
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

    timeout = _exc_of_kind(ExceptionKind.TIMEOUT)
    """Build a timeout (deadline exceeded) exception."""

    unavailable = _exc_of_kind(ExceptionKind.UNAVAILABLE)
    """Build an unavailable (switched off right now) exception."""

    # ....................... #

    def enrich(
        self,
        *,
        callsite: CallsiteFrame | JsonDict | None = None,
        resource: JsonDict | None = None,
        cause: JsonDict | None = None,
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
