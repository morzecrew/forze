from forze_s3._compat import require_s3

require_s3()

# ....................... #

from typing import Any, Mapping

from botocore import exceptions as s3_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _s3_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize low-level S3 / botocore errors into exc.internal hierarchy."""

    _ = site

    match exc:
        # --- connectivity / availability ---
        case s3_errors.EndpointConnectionError():
            return CoreException.infrastructure(
                "S3 endpoint connection error.",
                details=details,
            )

        case s3_errors.ConnectTimeoutError() | s3_errors.ReadTimeoutError():
            return CoreException.infrastructure(
                "S3 request timed out.",
                details=details,
            )

        # --- credentials / auth / ssl ---
        case s3_errors.NoCredentialsError() | s3_errors.PartialCredentialsError():
            return CoreException.infrastructure(
                "S3 credentials are not configured correctly.",
                details=details,
            )

        case s3_errors.SSLError():
            return CoreException.infrastructure(
                "S3 SSL error.",
                details=details,
            )

        # --- generic client-side error with code inspection ---
        case s3_errors.ClientError() as ce:
            resp: dict[str, Any] = getattr(ce, "response", {}) or {}
            err: dict[str, Any] = resp.get("Error") or {}
            code = str(err.get("Code") or "")

            # permissions / access
            if code in {"AccessDenied", "AccessDeniedException"}:
                return CoreException.infrastructure(
                    "S3 access denied.",
                    details=details,
                )

            # missing resources (bucket / key etc.)
            if code in {"NoSuchBucket", "NoSuchKey", "NotFound"}:
                return CoreException.infrastructure(
                    "S3 resource not found.",
                    details=details,
                )

            # throttling / rate limiting
            if code in {"SlowDown", "Throttling", "ThrottlingException"}:
                return CoreException.infrastructure(
                    "S3 request throttled.",
                    details=details,
                )

            # internal service errors
            if code in {"InternalError", "InternalServerError"}:
                return CoreException.infrastructure(
                    "S3 internal error.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"S3 client error ({code}).",
                details=details,
            )

        # --- broad fallback for other botocore errors ---
        case s3_errors.BotoCoreError() as be:
            return CoreException.infrastructure(
                "S3 core error.",
                details={**(details or {}), "error": str(be)},
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("S3", _s3_eh)
