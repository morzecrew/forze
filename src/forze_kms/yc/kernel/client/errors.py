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
    # way. A permanent denial still terminates — it exhausts the supervisor's
    # consecutive-crash ceiling.
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

    if code is grpc.StatusCode.FAILED_PRECONDITION:
        return CoreException.configuration(
            "Yandex Cloud KMS key is disabled or in an invalid state.",
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
