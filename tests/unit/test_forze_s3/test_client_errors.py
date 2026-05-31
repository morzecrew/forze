"""Unit tests for :mod:`forze_s3.kernel.client.errors`."""

from forze.base.exceptions import CoreException, ExceptionKind, exc
import pytest

pytest.importorskip("botocore")

from botocore import exceptions as s3_errors

from forze_s3.kernel.client.errors import _s3_eh

def _client_error(code: str) -> s3_errors.ClientError:
    return s3_errors.ClientError(
        error_response={"Error": {"Code": code, "Message": "x"}},
        operation_name="Test",
    )

class TestS3ErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert _s3_eh(original, site="op") is original

    def test_endpoint_connection_error(self) -> None:
        r = _s3_eh(s3_errors.EndpointConnectionError(endpoint_url="http://x"), site="put")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "connection" in r.summary.lower()

    def test_timeouts(self) -> None:
        r1 = _s3_eh(s3_errors.ConnectTimeoutError(endpoint_url="http://x"), site="x")
        r2 = _s3_eh(s3_errors.ReadTimeoutError(endpoint_url="http://x"), site="x")
        assert isinstance(r1, CoreException) and r1.kind == ExceptionKind.INFRASTRUCTURE
        assert "timed out" in r1.summary.lower()
        assert isinstance(r2, CoreException) and r2.kind == ExceptionKind.INFRASTRUCTURE
        assert "timed out" in r2.summary.lower()

    def test_credentials_errors(self) -> None:
        r = _s3_eh(s3_errors.NoCredentialsError(), site="x")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "credentials" in r.summary.lower()

    def test_ssl_error(self) -> None:
        r = _s3_eh(
            s3_errors.SSLError(endpoint_url="https://x", error="bad cert"),
            site="x",
        )
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "ssl" in r.summary.lower()

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
        r = _s3_eh(_client_error(code), site="op")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert needle in r.summary.lower()

    def test_botocore_fallback(self) -> None:
        r = _s3_eh(s3_errors.BotoCoreError(), site="op")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "core error" in r.summary.lower()

    def test_generic_fallback(self) -> None:
        r = _s3_eh(ValueError("nope"), site="s3_op")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "s3_op" in r.summary.lower()
