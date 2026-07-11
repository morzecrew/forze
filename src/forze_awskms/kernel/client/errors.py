from forze_awskms._compat import require_awskms

require_awskms()

# ....................... #

from typing import Any, Mapping

from botocore import exceptions as boto_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)
from forze.base.primitives import JsonDict

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _awskms_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize low-level AWS KMS / botocore errors into the exc hierarchy."""

    _ = site

    match exc:
        # --- connectivity / availability ---
        case boto_errors.EndpointConnectionError():
            return CoreException.infrastructure(
                "AWS KMS endpoint connection error.",
                details=details,
            )

        case boto_errors.ConnectTimeoutError() | boto_errors.ReadTimeoutError():
            return CoreException.infrastructure(
                "AWS KMS request timed out.",
                details=details,
            )

        # --- credentials / auth / ssl ---
        case boto_errors.NoCredentialsError() | boto_errors.PartialCredentialsError():
            return CoreException.infrastructure(
                "AWS KMS credentials are not configured correctly.",
                details=details,
            )

        case boto_errors.SSLError():
            return CoreException.infrastructure(
                "AWS KMS SSL error.",
                details=details,
            )

        # --- generic client-side error with code inspection ---
        case boto_errors.ClientError() as ce:
            resp: JsonDict = getattr(ce, "response", {}) or {}
            err: JsonDict = resp.get("Error") or {}
            code = str(err.get("Code") or "")

            # A corrupt / foreign wrapped data key, or one named under the wrong
            # CMK, is caller/data-caused — surface it as validation so it is not
            # masked as a 500 (the keyring's confused-deputy guard normally
            # rejects a foreign key_id before we ever reach KMS).
            if code in {"InvalidCiphertextException", "IncorrectKeyException"}:
                return CoreException.validation(
                    "AWS KMS could not decrypt the wrapped data key.",
                    code="core.crypto.wrapped_key_invalid",
                    details=details,
                )

            if code in {"AccessDeniedException", "KMSAccessDeniedException"}:
                return CoreException.infrastructure(
                    "AWS KMS access denied.",
                    details=details,
                )

            if code in {"NotFoundException", "KeyUnavailableException"}:
                return CoreException.infrastructure(
                    "AWS KMS key not found or unavailable.",
                    details=details,
                )

            if code in {"DisabledException", "KMSInvalidStateException"}:
                return CoreException.infrastructure(
                    "AWS KMS key is disabled or in an invalid state.",
                    details=details,
                )

            if code in {"ThrottlingException", "LimitExceededException"}:
                return CoreException.infrastructure(
                    "AWS KMS request throttled.",
                    details=details,
                )

            if code in {"KMSInternalException", "InternalError"}:
                return CoreException.infrastructure(
                    "AWS KMS internal error.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"AWS KMS client error ({code}).",
                details=details,
            )

        # --- broad fallback for other botocore errors ---
        case boto_errors.BotoCoreError() as be:
            return CoreException.infrastructure(
                "AWS KMS core error.",
                details={**(details or {}), "error": str(be)},
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("AWS_KMS", _awskms_eh)
