from typing import Mapping

import attrs

from .model import ExceptionKind

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
    ExceptionKind.INTERNAL: ExceptionKindEgress(
        expose_details=False,
        retryable=False,
    ),
}

# ....................... #


def exception_egress_policy(kind: ExceptionKind) -> ExceptionKindEgress:
    """Get the egress policy for a given exception kind."""

    return _EXC_KIND_POLICY.get(kind, _EXC_KIND_POLICY[ExceptionKind.INTERNAL])
