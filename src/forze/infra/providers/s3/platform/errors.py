from functools import partial
from typing import Any

from botocore import exceptions as s3_errors

from forze.base.errors import CoreError, error_handler, handled
from forze.infra.errors import InfrastructureError

# ----------------------- #


@error_handler
def _s3_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Normalize low-level S3 / botocore errors into CoreError hierarchy."""

    match e:
        case CoreError():
            return e

        # --- connectivity / availability ---
        case s3_errors.EndpointConnectionError():
            return InfrastructureError("S3 endpoint connection error.")

        case s3_errors.ConnectTimeoutError() | s3_errors.ReadTimeoutError():
            return InfrastructureError("S3 request timed out.")

        # --- credentials / auth / ssl ---
        case s3_errors.NoCredentialsError() | s3_errors.PartialCredentialsError():
            return InfrastructureError("S3 credentials are not configured correctly.")

        case s3_errors.SSLError():
            return InfrastructureError("S3 SSL error.")

        # --- generic client-side error with code inspection ---
        case s3_errors.ClientError() as ce:
            resp: dict[str, Any] = getattr(ce, "response", {}) or {}
            err: dict[str, Any] = resp.get("Error") or {}
            code = str(err.get("Code") or "")

            # permissions / access
            if code in {"AccessDenied", "AccessDeniedException"}:
                return InfrastructureError("S3 access denied.")

            # missing resources (bucket / key etc.)
            if code in {"NoSuchBucket", "NoSuchKey", "NotFound"}:
                return InfrastructureError("S3 resource not found.")

            # throttling / rate limiting
            if code in {"SlowDown", "Throttling", "ThrottlingException"}:
                return InfrastructureError("S3 request throttled.")

            # internal service errors
            if code in {"InternalError", "InternalServerError"}:
                return InfrastructureError("S3 internal error.")

            return InfrastructureError(f"S3 client error ({code}).")

        # --- broad fallback for other botocore errors ---
        case s3_errors.BotoCoreError():
            return InfrastructureError(f"S3 core error: {e}")

        # --- ultimate fallback ---
        case _:
            return InfrastructureError(
                f"An error occurred while executing S3 operation {op}: {e}"
            )


# ----------------------- #

s3_handled = partial(handled, _s3_eh)
