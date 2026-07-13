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

        case gcp_errors.PermissionDenied() | gcp_errors.Unauthenticated():
            return CoreException.infrastructure(
                "GCP KMS access denied.",
                details=details,
            )

        case gcp_errors.NotFound():
            return CoreException.infrastructure(
                "GCP KMS key not found.",
                details=details,
            )

        case gcp_errors.FailedPrecondition():
            return CoreException.infrastructure(
                "GCP KMS key is disabled or in an invalid state.",
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
