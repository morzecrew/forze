from collections.abc import Mapping
from typing import Literal

import attrs

from .model import CoreException, ExceptionKind

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class ExceptionKindEgress:
    """Egress for :class:`ExceptionKind`."""

    expose_details: bool
    """Whether to expose the details of the exception."""

    retryable: bool
    """Whether the exception is retryable."""


# ....................... #

_EXC_KIND_POLICY: Mapping[ExceptionKind, ExceptionKindEgress] = {
    ExceptionKind.NOT_FOUND: ExceptionKindEgress(
        expose_details=True,
        retryable=False,
    ),
    ExceptionKind.CONFLICT: ExceptionKindEgress(
        expose_details=True,
        retryable=False,
    ),
    ExceptionKind.CONCURRENCY: ExceptionKindEgress(
        expose_details=True,
        retryable=True,
    ),
    ExceptionKind.VALIDATION: ExceptionKindEgress(
        expose_details=True,
        retryable=False,
    ),
    ExceptionKind.DOMAIN: ExceptionKindEgress(
        expose_details=True,
        retryable=False,
    ),
    ExceptionKind.PRECONDITION: ExceptionKindEgress(
        expose_details=True,
        retryable=False,
    ),
    ExceptionKind.AUTHENTICATION: ExceptionKindEgress(
        expose_details=False,
        retryable=False,
    ),
    ExceptionKind.AUTHORIZATION: ExceptionKindEgress(
        expose_details=False,
        retryable=False,
    ),
    ExceptionKind.CONFIGURATION: ExceptionKindEgress(
        # Configuration errors carry internal wiring info (dep keys, policy
        # names) that must never reach clients.
        expose_details=False,
        retryable=False,
    ),
    ExceptionKind.INFRASTRUCTURE: ExceptionKindEgress(
        expose_details=False,
        retryable=True,
    ),
    ExceptionKind.THROTTLED: ExceptionKindEgress(
        # Throttle details carry wiring info (policy names, routes) that
        # must never reach clients. Retryable: capacity refills over time,
        # so a Retry strategy may legitimately wait out a rate limit.
        expose_details=False,
        retryable=True,
    ),
    ExceptionKind.TIMEOUT: ExceptionKindEgress(
        # Deadline exceeded: the invocation's time budget is spent, so an
        # in-process retry under the same deadline is pointless. Details may
        # carry wiring info (policy names, routes).
        expose_details=False,
        retryable=False,
    ),
    ExceptionKind.INTERNAL: ExceptionKindEgress(
        expose_details=False,
        retryable=False,
    ),
}

# ....................... #


def exception_egress_policy(kind: ExceptionKind) -> ExceptionKindEgress:
    """Get the egress policy for a given exception kind."""

    return _EXC_KIND_POLICY.get(kind, _EXC_KIND_POLICY[ExceptionKind.INTERNAL])


# ....................... #

_EXC_KIND_HTTP_STATUS: Mapping[ExceptionKind, int] = {
    ExceptionKind.NOT_FOUND: 404,
    ExceptionKind.CONFLICT: 409,
    ExceptionKind.CONCURRENCY: 409,
    ExceptionKind.VALIDATION: 422,
    ExceptionKind.DOMAIN: 400,
    ExceptionKind.PRECONDITION: 400,
    ExceptionKind.AUTHENTICATION: 401,
    ExceptionKind.AUTHORIZATION: 403,
    ExceptionKind.THROTTLED: 429,
    ExceptionKind.TIMEOUT: 504,
}

# ....................... #


def http_status_for_kind(kind: ExceptionKind) -> int:
    """Map an :class:`ExceptionKind` to its conventional HTTP status code.

    Kinds with no client-facing status of their own — internal, infrastructure,
    configuration — map to ``500``. Transport-agnostic: any HTTP-serving layer
    (FastAPI, MCP, …) can reuse it to turn a :class:`CoreException` kind into a
    status code.
    """

    return _EXC_KIND_HTTP_STATUS.get(kind, 500)


# ....................... #

_COLLAPSIBLE_KINDS = frozenset({ExceptionKind.AUTHORIZATION, ExceptionKind.NOT_FOUND})


@attrs.define(slots=True, frozen=True, kw_only=True)
class DenialPosture:
    """How an error about a resource renders to clients.

    ``standard`` (the default) renders every error as it is. ``non_disclosing`` renders an
    ``authorization`` or ``not_found`` error whose
    :attr:`~forze.base.exceptions.CoreException.resource_type` is in :attr:`resource_types`
    as one canonical not-found — same status, body and code whether the row is missing or
    the caller may not see it — so the response is no longer an existence check. The
    server-side exception keeps its real kind. This closes the response-shape oracle, not
    the timing one.
    """

    mode: Literal["standard", "non_disclosing"] = "standard"
    """``standard`` renders errors as they are; ``non_disclosing`` collapses covered ones."""

    resource_types: frozenset[str] = attrs.field(default=frozenset(), converter=frozenset)
    """The resource types (document spec names) a ``non_disclosing`` posture covers."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.mode == "non_disclosing" and not self.resource_types:
            raise CoreException.configuration(
                "A non_disclosing DenialPosture must name the resource types it covers.",
                code="denial_posture_empty",
            )

    # ....................... #

    def collapses(self, exc: CoreException) -> bool:
        """Whether *exc* renders as the canonical not-found under this posture."""

        return (
            self.mode == "non_disclosing"
            and exc.kind in _COLLAPSIBLE_KINDS
            and exc.resource_type in self.resource_types
        )


_denial_posture = DenialPosture()


def configure_denial_posture(posture: DenialPosture) -> DenialPosture:
    """Set the process-wide :class:`DenialPosture`; return the previous one.

    Process-wide on purpose: every transport renders errors in its own request task, which
    a context variable bound at startup would not reach. An
    :class:`~forze.application.execution.ExecutionRuntime` built with a posture sets it
    for its scope and restores the previous one on exit.
    """

    global _denial_posture

    previous, _denial_posture = _denial_posture, posture
    return previous


def current_denial_posture() -> DenialPosture:
    """Return the process-wide :class:`DenialPosture` (``standard`` unless configured)."""

    return _denial_posture
