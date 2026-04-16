"""Unit tests for the S3 / botocore error handler."""

import pytest

pytest.importorskip("botocore")

from botocore import exceptions as s3_errors

from forze.base.errors import CoreError, InfrastructureError
from forze_s3.kernel.platform.errors import _s3_eh


def _client_error(code: str) -> s3_errors.ClientError:
    return s3_errors.ClientError(
        error_response={"Error": {"Code": code, "Message": "x"}},
        operation_name="Test",
    )


class TestS3ErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = CoreError("x")
        assert _s3_eh(original, "op") is original

    def test_endpoint_connection_error(self) -> None:
        r = _s3_eh(s3_errors.EndpointConnectionError(endpoint_url="http://x"), "put")
        assert isinstance(r, InfrastructureError)
        assert "connection" in r.code.lower()

    def test_timeouts(self) -> None:
        r1 = _s3_eh(s3_errors.ConnectTimeoutError(endpoint_url="http://x"), "x")
        r2 = _s3_eh(s3_errors.ReadTimeoutError(endpoint_url="http://x"), "x")
        assert isinstance(r1, InfrastructureError)
        assert "timed out" in r1.code.lower()
        assert isinstance(r2, InfrastructureError)
        assert "timed out" in r2.code.lower()

    def test_credentials_errors(self) -> None:
        r = _s3_eh(s3_errors.NoCredentialsError(), "x")
        assert isinstance(r, InfrastructureError)
        assert "credentials" in r.code.lower()

    def test_ssl_error(self) -> None:
        r = _s3_eh(
            s3_errors.SSLError(endpoint_url="https://x", error="bad cert"),
            "x",
        )
        assert isinstance(r, InfrastructureError)
        assert "ssl" in r.code.lower()

    @pytest.mark.parametrize(
        ("code", "needle"),
        [
            ("AccessDenied", "access denied"),
            ("NoSuchBucket", "not found"),
            ("NoSuchKey", "not found"),
            ("NotFound", "not found"),
            ("SlowDown", "throttl"),
            ("Throttling", "throttl"),
            ("InternalError", "internal error"),
            ("UnknownCode", "unknowncode"),
        ],
    )
    def test_client_error_codes(self, code: str, needle: str) -> None:
        r = _s3_eh(_client_error(code), "op")
        assert isinstance(r, InfrastructureError)
        assert needle in r.code.lower()

    def test_botocore_fallback(self) -> None:
        r = _s3_eh(s3_errors.BotoCoreError(), "op")
        assert isinstance(r, InfrastructureError)
        assert "core error" in r.code.lower()

    def test_generic_fallback(self) -> None:
        r = _s3_eh(ValueError("nope"), "s3_op")
        assert isinstance(r, InfrastructureError)
        assert "s3_op" in r.code
