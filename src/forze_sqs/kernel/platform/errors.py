from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

from functools import partial
from typing import Any

from botocore import exceptions as sqs_errors

from forze.base.errors import CoreError, InfrastructureError, error_handler, handled

# ----------------------- #


@error_handler
def _sqs_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    """Normalize low-level SQS/botocore errors into CoreError hierarchy."""

    match e:
        case CoreError():
            return e

        case sqs_errors.EndpointConnectionError():
            return InfrastructureError("SQS endpoint connection error.")

        case sqs_errors.ConnectTimeoutError() | sqs_errors.ReadTimeoutError():
            return InfrastructureError("SQS request timed out.")

        case sqs_errors.NoCredentialsError() | sqs_errors.PartialCredentialsError():
            return InfrastructureError("SQS credentials are not configured correctly.")

        case sqs_errors.SSLError():
            return InfrastructureError("SQS SSL error.")

        case sqs_errors.ClientError() as ce:
            resp: dict[str, Any] = getattr(ce, "response", {}) or {}
            err: dict[str, Any] = resp.get("Error") or {}
            code = str(err.get("Code") or "")

            if code in {"AccessDenied", "AccessDeniedException"}:
                return InfrastructureError("SQS access denied.")

            if code in {
                "QueueDoesNotExist",
                "AWS.SimpleQueueService.NonExistentQueue",
                "ResourceNotFoundException",
            }:
                return InfrastructureError("SQS queue does not exist.")

            if code in {
                "Throttling",
                "ThrottlingException",
                "RequestThrottled",
                "TooManyRequestsException",
            }:
                return InfrastructureError("SQS request throttled.")

            if code in {"InternalError", "InternalFailure", "ServiceUnavailable"}:
                return InfrastructureError("SQS internal service error.")

            return InfrastructureError(f"SQS client error ({code}).")

        case sqs_errors.BotoCoreError():
            return InfrastructureError(f"SQS core error: {e}")

        case _:
            return InfrastructureError(
                f"An error occurred while executing SQS operation {op}: {e}"
            )


# ----------------------- #

sqs_handled = partial(handled, _sqs_eh)
