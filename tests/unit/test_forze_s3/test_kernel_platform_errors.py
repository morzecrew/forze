"""Unit tests for S3 platform error mapping."""

import pytest
from botocore import exceptions as s3_errors

from forze.base.errors import CoreError, InfrastructureError

from forze_s3.kernel.platform.errors import _s3_eh


def test_s3_eh_passes_through_core_error() -> None:
    err = CoreError("x")
    assert _s3_eh(err, "op") is err


@pytest.mark.parametrize(
    ("exc", "msg"),
    [
        (s3_errors.EndpointConnectionError(endpoint_url="http://x"), "S3 endpoint connection error."),
        (s3_errors.ConnectTimeoutError(endpoint_url="http://x"), "S3 request timed out."),
        (s3_errors.ReadTimeoutError(endpoint_url="http://x"), "S3 request timed out."),
        (s3_errors.NoCredentialsError(), "S3 credentials are not configured correctly."),
        (s3_errors.PartialCredentialsError(provider="p", cred_var="c"), "S3 credentials are not configured correctly."),
        (s3_errors.SSLError(endpoint_url="http://x", error="e"), "S3 SSL error."),
    ],
)
def test_s3_eh_infra_connectivity(exc: Exception, msg: str) -> None:
    out = _s3_eh(exc, "get_object")
    assert isinstance(out, InfrastructureError)
    assert out.code == msg


@pytest.mark.parametrize(
    ("code", "msg"),
    [
        ("AccessDenied", "S3 access denied."),
        ("NoSuchBucket", "S3 resource not found."),
        ("NoSuchKey", "S3 resource not found."),
        ("NotFound", "S3 resource not found."),
        ("SlowDown", "S3 request throttled."),
        ("Throttling", "S3 request throttled."),
        ("InternalError", "S3 internal error."),
        ("UnknownCode", "S3 client error (UnknownCode)."),
    ],
)
def test_s3_eh_client_error_codes(code: str, msg: str) -> None:
    ce = s3_errors.ClientError({"Error": {"Code": code, "Message": "m"}}, "GetObject")
    out = _s3_eh(ce, "head")
    assert out.code == msg


def test_s3_eh_client_error_missing_error_block() -> None:
    ce = s3_errors.ClientError({}, "GetObject")
    out = _s3_eh(ce, "x")
    assert out.code == "S3 client error ()."


def test_s3_eh_botocore_generic() -> None:
    exc = s3_errors.BotoCoreError()
    out = _s3_eh(exc, "y")
    assert out.code.startswith("S3 core error:")


def test_s3_eh_fallback() -> None:
    out = _s3_eh(ValueError("z"), "my_op")
    assert "my_op" in out.code
    assert "z" in out.code
