import pytest
from botocore import exceptions as sqs_errors
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
)

from forze.base.errors import CoreError, InfrastructureError
from forze_sqs.kernel.platform.errors import _sqs_eh, sqs_handled


def _client_error(code: str) -> sqs_errors.ClientError:
    return sqs_errors.ClientError(
        error_response={"Error": {"Code": code, "Message": "x"}},
        operation_name="Test",
    )


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


class TestSqsErrorHandlerDirect:
    """Direct tests for :func:`_sqs_eh` (branch coverage)."""

    def test_core_error_passthrough(self) -> None:
        original = CoreError("x")
        assert _sqs_eh(original, "op") is original

    def test_read_timeout_maps(self) -> None:
        r = _sqs_eh(sqs_errors.ReadTimeoutError(endpoint_url="http://x"), "x")
        assert "timed out" in r.code.lower()

    def test_partial_credentials(self) -> None:
        r = _sqs_eh(
            sqs_errors.PartialCredentialsError(provider="env", cred_var="AWS_SECRET_ACCESS_KEY"),
            "x",
        )
        assert "credentials" in r.code.lower()

    def test_ssl_error(self) -> None:
        r = _sqs_eh(
            sqs_errors.SSLError(endpoint_url="https://x", error="e"),
            "x",
        )
        assert "ssl" in r.code.lower()

    @pytest.mark.parametrize(
        ("code", "needle"),
        [
            ("AccessDenied", "access denied"),
            ("AccessDeniedException", "access denied"),
            ("QueueDoesNotExist", "queue does not exist"),
            ("AWS.SimpleQueueService.NonExistentQueue", "queue does not exist"),
            ("ResourceNotFoundException", "queue does not exist"),
            ("Throttling", "throttl"),
            ("ThrottlingException", "throttl"),
            ("RequestThrottled", "throttl"),
            ("TooManyRequestsException", "throttl"),
            ("InternalError", "internal"),
            ("InternalFailure", "internal"),
            ("ServiceUnavailable", "internal"),
            ("UnknownCode", "unknowncode"),
        ],
    )
    def test_client_error_codes(self, code: str, needle: str) -> None:
        r = _sqs_eh(_client_error(code), "op")
        assert needle in r.code.lower()

    def test_botocore_fallback(self) -> None:
        r = _sqs_eh(sqs_errors.BotoCoreError(), "op")
        assert "core error" in r.code.lower()

    def test_generic_fallback(self) -> None:
        r = _sqs_eh(ValueError("bad"), "sqs_op")
        assert "sqs_op" in r.code
