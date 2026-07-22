from forze_kms.gcp._compat import require_kms_gcp

require_kms_gcp()

# ....................... #

from collections.abc import Mapping
from typing import Any

from google.api_core import exceptions as gcp_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #

_CRYPTO_SITES = frozenset({"gcpkms.encrypt", "gcpkms.decrypt"})
"""Sites where an ``InvalidArgument`` means the *ciphertext* was rejected, as opposed to a
malformed key ring / key id on a key-administration call."""


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _gcpkms_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize low-level GCP KMS (google-api-core) errors into the exc hierarchy."""

    match exc:
        case gcp_errors.InvalidArgument():
            # Both the crypto ops and key administration raise InvalidArgument, so the
            # call site decides what it means: on encrypt/decrypt it is a corrupt or
            # foreign wrapped data key (caller/data-caused — the keyring's
            # confused-deputy guard normally rejects a foreign key_id first), while on
            # a provisioning call it is a malformed key ring or key id, which must not
            # be dressed up as a ciphertext failure.
            if site in _CRYPTO_SITES:
                return CoreException.validation(
                    "GCP KMS rejected the ciphertext as invalid.",
                    code="core.crypto.wrapped_key_invalid",
                    details=details,
                )

            return CoreException.validation(
                "GCP KMS rejected the request as invalid.",
                details=details,
            )

        # Access denied stays retryable, unlike the key-state errors below: IAM bindings
        # propagate eventually, so a freshly granted principal is denied for seconds
        # before it is allowed. Non-retryable here would pause the consumer on a grant
        # already on its way. A permanent denial still terminates — it exhausts the
        # supervisor's consecutive-crash ceiling.
        case gcp_errors.PermissionDenied() | gcp_errors.Unauthenticated():
            return CoreException.infrastructure(
                "GCP KMS access denied.",
                details=details,
            )

        # --- permanent: retrying never clears these, an operator must act ---
        # Configuration rather than infrastructure so the egress policy reports them
        # non-retryable: as infrastructure they drove a decrypt loop to crash-restart
        # forever on a key that is never coming back. Details stay hidden either way.
        case gcp_errors.NotFound():
            return CoreException.configuration(
                "GCP KMS key not found — it is missing or has been destroyed.",
                details=details,
            )

        case gcp_errors.FailedPrecondition():
            return CoreException.configuration(
                "GCP KMS key version is disabled or destroyed.",
                details=details,
            )

        case gcp_errors.ResourceExhausted():
            return CoreException.infrastructure(
                "GCP KMS request throttled (quota exhausted).",
                details=details,
            )

        case (
            gcp_errors.ServiceUnavailable()
            | gcp_errors.DeadlineExceeded()
            | gcp_errors.RetryError()
        ):
            return CoreException.infrastructure(
                "GCP KMS is unavailable.",
                details=details,
            )

        case gcp_errors.GoogleAPICallError() as api_error:
            return CoreException.infrastructure(
                f"GCP KMS API error ({api_error.code}).",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("GCP_KMS", _gcpkms_eh)
