from forze_kms.gcp._compat import require_kms_gcp

require_kms_gcp()

# ....................... #

from typing import Any, Mapping

from google.api_core import exceptions as gcp_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _gcpkms_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize low-level GCP KMS (google-api-core) errors into the exc hierarchy."""

    _ = site

    match exc:
        # A corrupt / foreign wrapped data key surfaces as InvalidArgument — that
        # is caller/data-caused, so classify it as validation (the keyring's
        # confused-deputy guard normally rejects a foreign key_id before KMS).
        case gcp_errors.InvalidArgument():
            return CoreException.validation(
                "GCP KMS rejected the ciphertext as invalid.",
                code="core.crypto.wrapped_key_invalid",
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
