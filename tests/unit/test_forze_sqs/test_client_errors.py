"""Unit tests for :mod:`forze_sqs.kernel.client.errors`."""

import pytest
from botocore import exceptions as sqs_errors
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
)

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_sqs.kernel.client.errors import _sqs_eh

# ----------------------- #


def _client_error(code: str) -> sqs_errors.ClientError:
    return sqs_errors.ClientError(
        error_response={"Error": {"Code": code, "Message": "x"}},
        operation_name="Test",
    )


class TestSqsErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert _sqs_eh(original, site="op") is original

    @pytest.mark.parametrize(
        ("raised", "needle"),
        [
            (
                EndpointConnectionError(endpoint_url="http://localhost:4566"),
                "endpoint connection error",
            ),
            (ConnectTimeoutError(endpoint_url="http://localhost:4566"), "timed out"),
            (NoCredentialsError(), "credentials"),
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
    def test_maps_known_exceptions(self, raised: Exception, needle: str) -> None:
        r = _sqs_eh(raised, site="sqs.test")
        assert r is not None
        assert r.kind == ExceptionKind.INFRASTRUCTURE
        assert needle in r.summary.lower()

    def test_unknown_exception_fallback(self) -> None:
        r = _sqs_eh(RuntimeError("boom"), site="sqs.test")
        assert r is not None
        assert "sqs.test" in r.summary.lower()
        # raw driver text must not leak into the summary, only into details
        assert "boom" not in r.summary
        assert r.details is not None
        assert r.details["error"] == "boom"

    def test_read_timeout_maps(self) -> None:
        r = _sqs_eh(sqs_errors.ReadTimeoutError(endpoint_url="http://x"), site="x")
        assert r is not None
        assert "timed out" in r.summary.lower()

    def test_partial_credentials(self) -> None:
        r = _sqs_eh(
            sqs_errors.PartialCredentialsError(
                provider="env", cred_var="AWS_SECRET_ACCESS_KEY"
            ),
            site="x",
        )
        assert r is not None
        assert "credentials" in r.summary.lower()

    def test_ssl_error(self) -> None:
        r = _sqs_eh(
            sqs_errors.SSLError(endpoint_url="https://x", error="e"),
            site="x",
        )
        assert r is not None
        assert "ssl" in r.summary.lower()

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
        r = _sqs_eh(_client_error(code), site="op")
        assert r is not None
        assert needle in r.summary.lower()

    def test_botocore_fallback(self) -> None:
        raised = sqs_errors.BotoCoreError()
        r = _sqs_eh(raised, site="op")
        assert r is not None
        assert "core error" in r.summary.lower()
        assert str(raised) not in r.summary
        assert r.details is not None
        assert r.details["error"] == str(raised)


class TestAssembledChain:
    """Regression: the package mapper must be reachable through the chain
    wired into ``exc_interceptor`` (nested default chain used to shadow it)."""

    def test_endpoint_error_through_assembled_chain(self) -> None:
        from forze_sqs.kernel.client.errors import exc_interceptor

        out = exc_interceptor.mapper(
            EndpointConnectionError(endpoint_url="http://localhost:4566"),
            site="send",
        )
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code != "core.unhandled"
        assert "connection" in out.summary.lower()
