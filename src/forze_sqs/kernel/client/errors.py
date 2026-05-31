from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

from typing import Any, Mapping

from botocore import exceptions as sqs_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    ExceptionMapper,
    default_chain_exc_mapper,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _sqs_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    """Normalize low-level SQS/botocore errors into exc.internal hierarchy."""

    match exc:
        case CoreException():
            return exc

        case sqs_errors.EndpointConnectionError():
            return CoreException.infrastructure(
                "SQS endpoint connection error.",
                details=details,
            )

        case sqs_errors.ConnectTimeoutError() | sqs_errors.ReadTimeoutError():
            return CoreException.infrastructure(
                "SQS request timed out.",
                details=details,
            )

        case sqs_errors.NoCredentialsError() | sqs_errors.PartialCredentialsError():
            return CoreException.infrastructure(
                "SQS credentials are not configured correctly.",
                details=details,
            )

        case sqs_errors.SSLError():
            return CoreException.infrastructure(
                "SQS SSL error.",
                details=details,
            )

        case sqs_errors.ClientError() as ce:
            resp: dict[str, Any] = getattr(ce, "response", {}) or {}
            err: dict[str, Any] = resp.get("Error") or {}
            code = str(err.get("Code") or "")

            if code in {"AccessDenied", "AccessDeniedException"}:
                return CoreException.infrastructure(
                    "SQS access denied.",
                    details=details,
                )

            if code in {
                "QueueDoesNotExist",
                "AWS.SimpleQueueService.NonExistentQueue",
                "ResourceNotFoundException",
            }:
                return CoreException.infrastructure(
                    "SQS queue does not exist.",
                    details=details,
                )

            if code in {
                "Throttling",
                "ThrottlingException",
                "RequestThrottled",
                "TooManyRequestsException",
            }:
                return CoreException.infrastructure(
                    "SQS request throttled.",
                    details=details,
                )

            if code in {"InternalError", "InternalFailure", "ServiceUnavailable"}:
                return CoreException.infrastructure(
                    "SQS internal service error.",
                    details=details,
                )

            return CoreException.infrastructure(
                f"SQS client error ({code}).",
                details=details,
            )

        case sqs_errors.BotoCoreError() as be:
            return CoreException.infrastructure(
                f"SQS core error: {be}",
                details=details,
            )

        case _:
            return CoreException.infrastructure(
                f"An error occurred while executing SQS operation {site}: {exc}",
                details=details,
            )


# ....................... #

_sqs_chain = default_chain_exc_mapper.chain(_sqs_eh)
exc_interceptor = ExceptionInterceptor(mapper=_sqs_chain)
