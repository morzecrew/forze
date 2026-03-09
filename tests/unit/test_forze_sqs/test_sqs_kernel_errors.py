import pytest
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
)

from forze.base.errors import CoreError, InfrastructureError
from forze_sqs.kernel.platform.errors import sqs_handled


@sqs_handled("sqs.test")
async def _raise(exc: Exception) -> None:
    raise exc


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exc", "msg"),
    [
        (
            EndpointConnectionError(endpoint_url="http://localhost:4566"),
            "endpoint connection error",
        ),
        (ConnectTimeoutError(endpoint_url="http://localhost:4566"), "timed out"),
        (NoCredentialsError(), "credentials are not configured"),
        (
            ClientError(
                {
                    "Error": {
                        "Code": "AWS.SimpleQueueService.NonExistentQueue",
                        "Message": "missing",
                    }
                },
                "ReceiveMessage",
            ),
            "queue does not exist",
        ),
    ],
)
async def test_sqs_error_handler_maps_known_exceptions(
    exc: Exception,
    msg: str,
) -> None:
    with pytest.raises(InfrastructureError, match=msg):
        await _raise(exc)


@pytest.mark.asyncio
async def test_sqs_error_handler_passthrough_core_error() -> None:
    with pytest.raises(CoreError, match="already_core"):
        await _raise(CoreError("already_core"))


@pytest.mark.asyncio
async def test_sqs_error_handler_maps_unknown_exception() -> None:
    with pytest.raises(InfrastructureError, match="SQS operation sqs.test"):
        await _raise(RuntimeError("boom"))
