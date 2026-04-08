"""Unit tests for SQS platform error mapping."""

import pytest
from botocore import exceptions as sqs_errors

from forze.base.errors import CoreError, InfrastructureError

from forze_sqs.kernel.platform.errors import _sqs_eh


def test_sqs_eh_passes_through_core_error() -> None:
    err = CoreError("x")
    assert _sqs_eh(err, "op") is err


@pytest.mark.parametrize(
    ("exc", "msg"),
    [
        (sqs_errors.EndpointConnectionError(endpoint_url="http://x"), "SQS endpoint connection error."),
        (sqs_errors.ConnectTimeoutError(endpoint_url="http://x"), "SQS request timed out."),
        (sqs_errors.ReadTimeoutError(endpoint_url="http://x"), "SQS request timed out."),
        (sqs_errors.NoCredentialsError(), "SQS credentials are not configured correctly."),
        (sqs_errors.PartialCredentialsError(provider="p", cred_var="c"), "SQS credentials are not configured correctly."),
        (sqs_errors.SSLError(endpoint_url="http://x", error="e"), "SQS SSL error."),
    ],
)
def test_sqs_eh_infra(exc: Exception, msg: str) -> None:
    out = _sqs_eh(exc, "send")
    assert isinstance(out, InfrastructureError)
    assert out.code == msg


@pytest.mark.parametrize(
    ("code", "msg"),
    [
        ("AccessDenied", "SQS access denied."),
        ("QueueDoesNotExist", "SQS queue does not exist."),
        ("AWS.SimpleQueueService.NonExistentQueue", "SQS queue does not exist."),
        ("Throttling", "SQS request throttled."),
        ("RequestThrottled", "SQS request throttled."),
        ("InternalError", "SQS internal service error."),
        ("Other", "SQS client error (Other)."),
    ],
)
def test_sqs_eh_client_error_codes(code: str, msg: str) -> None:
    ce = sqs_errors.ClientError({"Error": {"Code": code, "Message": "m"}}, "SendMessage")
    out = _sqs_eh(ce, "op")
    assert out.code == msg


def test_sqs_eh_botocore_generic() -> None:
    out = _sqs_eh(sqs_errors.BotoCoreError(), "z")
    assert out.code.startswith("SQS core error:")


def test_sqs_eh_fallback() -> None:
    out = _sqs_eh(KeyError("k"), "recv")
    assert "recv" in out.code
