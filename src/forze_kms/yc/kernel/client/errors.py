from forze_kms.yc._compat import require_kms_yc

require_kms_yc()

# ....................... #

from collections.abc import Mapping
from typing import Any

import grpc

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #

_ACCESS_DENIED = frozenset({grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAUTHENTICATED})
_UNAVAILABLE = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})

_DEAD_KEY_STATES = ("INACTIVE", "SCHEDULED_FOR_DESTRUCTION", "DESTROYED")
"""Key / key-version states an operator must reverse; nothing about them is transient.

The names are the provider's own (``SymmetricKey.Status`` and
``SymmetricKeyVersion.Status``). ``CREATING`` is deliberately absent — it clears by itself
in seconds — and so is ``ACTIVE``, which is a substring of ``INACTIVE`` and would match it.
"""


def _status_detail(error: grpc.RpcError) -> str:
    """Status message of a gRPC error; empty when it carries no status.

    A bare ``grpc.RpcError`` is not necessarily a ``grpc.Call``, so ``details`` may be
    missing altogether.
    """

    if not callable(getattr(error, "details", None)):
        return ""

    try:
        return error.details() or ""

    except Exception:
        return ""


def _names_a_dead_key(error: grpc.RpcError) -> bool:
    """Whether a ``FAILED_PRECONDITION`` names a key state retrying cannot clear.

    The status carries no structured state field, so this is necessarily a text check over
    the status message, and an unrecognized one is treated as **transient** by the caller.
    That is the safe direction for a heuristic: a key that really is gone still gets
    escalated to a critical alert by the supervisor, whereas guessing "permanent" pauses a
    consumer for good over a key that was merely mid-creation or propagating a change.
    """

    detail = _status_detail(error).upper()

    return any(state in detail for state in _DEAD_KEY_STATES)


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _yckms_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize low-level Yandex Cloud KMS gRPC errors into the exc hierarchy."""

    _ = site

    if not isinstance(exc, grpc.RpcError):
        return None

    # ``grpc.RpcError`` from a sync call is also a ``grpc.Call`` carrying the status.
    code = exc.code() if callable(getattr(exc, "code", None)) else None

    # A corrupt / foreign wrapped data key surfaces as INVALID_ARGUMENT — that is
    # caller/data-caused, so classify it as validation (the keyring's confused-deputy
    # guard normally rejects a foreign key_id before we ever reach KMS).
    if code is grpc.StatusCode.INVALID_ARGUMENT:
        return CoreException.validation(
            "Yandex Cloud KMS rejected the ciphertext as invalid.",
            code="core.crypto.wrapped_key_invalid",
            details=details,
        )

    # Access denied stays retryable, unlike the key-state errors below: IAM bindings
    # propagate eventually, so a freshly granted principal is denied for seconds before it
    # is allowed. Non-retryable here would pause the consumer on a grant already on its
    # way. A permanent denial keeps retrying, but the supervisor escalates it to a
    # critical alert once it stops looking transient.
    if code in _ACCESS_DENIED:
        return CoreException.infrastructure(
            "Yandex Cloud KMS access denied.",
            details=details,
        )

    # --- permanent: retrying never clears these, an operator must act ---
    # Configuration rather than infrastructure so the egress policy reports them
    # non-retryable: as infrastructure they drove a decrypt loop to crash-restart
    # forever on a key that is never coming back. Details stay hidden either way.
    if code is grpc.StatusCode.NOT_FOUND:
        return CoreException.configuration(
            "Yandex Cloud KMS key not found — it is missing or has been deleted.",
            details=details,
        )

    # A precondition failure covers both sides of the line, so it turns on the state the
    # message names: INACTIVE / SCHEDULED_FOR_DESTRUCTION / DESTROYED is an operator's to
    # reverse, while CREATING — or a message naming no state at all — clears on its own and
    # must stay retryable. Ambiguity resolves toward retrying: calling it permanent strands
    # a consumer for good on a key that was merely mid-creation.
    if code is grpc.StatusCode.FAILED_PRECONDITION:
        if _names_a_dead_key(exc):
            return CoreException.configuration(
                "Yandex Cloud KMS key is disabled or scheduled for destruction.",
                details=details,
            )

        return CoreException.infrastructure(
            "Yandex Cloud KMS key is not currently usable.",
            details=details,
        )

    if code is grpc.StatusCode.RESOURCE_EXHAUSTED:
        return CoreException.infrastructure(
            "Yandex Cloud KMS request throttled (quota exhausted).",
            details=details,
        )

    if code in _UNAVAILABLE:
        return CoreException.infrastructure(
            "Yandex Cloud KMS is unavailable.",
            details=details,
        )

    return CoreException.infrastructure(
        f"Yandex Cloud KMS API error ({code}).",
        details=details,
    )


# ....................... #

exc_interceptor = build_exc_interceptor("YC_KMS", _yckms_eh)
