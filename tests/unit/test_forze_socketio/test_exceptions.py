"""Unit tests for :mod:`forze_socketio.exceptions`."""

import io
import json

from forze.base.exceptions import ExceptionKind, exc
from forze.base.scrubbing import SECRET_PLACEHOLDER
from forze_socketio.exceptions import (
    GENERIC_INTERNAL_DETAIL,
    INTERNAL_ERROR_CODE,
    build_core_exception_ack,
    build_unhandled_exception_ack,
    is_server_error_kind,
)

# ----------------------- #


def json_records(stream: io.StringIO) -> list[dict]:
    out: list[dict] = []
    for line in stream.getvalue().strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            out.append(json.loads(line))
    return out


# ----------------------- #


class TestServerErrorKindMapping:
    def test_client_safe_kinds_are_not_server_errors(self) -> None:
        for kind in (
            ExceptionKind.NOT_FOUND,
            ExceptionKind.CONFLICT,
            ExceptionKind.VALIDATION,
            ExceptionKind.DOMAIN,
            ExceptionKind.PRECONDITION,
            ExceptionKind.AUTHENTICATION,
            ExceptionKind.AUTHORIZATION,
        ):
            assert is_server_error_kind(kind) is False

    def test_server_side_kinds_are_server_errors(self) -> None:
        for kind in (
            ExceptionKind.INTERNAL,
            ExceptionKind.INFRASTRUCTURE,
            ExceptionKind.CONFIGURATION,
            ExceptionKind.CONCURRENCY,
        ):
            assert is_server_error_kind(kind) is True


# ....................... #


class TestBuildCoreExceptionAck:
    def test_validation_exposes_summary_and_sanitized_context(self) -> None:
        err = exc.validation(
            "Text must not be empty",
            details={"field": "text", "token": "super-secret"},
        )

        ack = build_core_exception_ack(err)

        assert ack == {
            "error": {
                "detail": "Text must not be empty",
                "code": "core.validation",
                "kind": "validation",
                "context": {"field": "text", "token": SECRET_PLACEHOLDER},
            }
        }

    def test_authentication_hides_details(self) -> None:
        err = exc.authentication(
            "Invalid token",
            details={"resolver": "jwt"},
        )

        ack = build_core_exception_ack(err)

        assert ack == {
            "error": {
                "detail": "Invalid token",
                "code": "core.authentication",
                "kind": "authentication",
            }
        }

    def test_client_safe_kind_does_not_log(self, error_log_buf: io.StringIO) -> None:
        build_core_exception_ack(exc.not_found("Document not found"))

        assert json_records(error_log_buf) == []

    def test_infrastructure_acked_generic_and_logged(
        self, error_log_buf: io.StringIO
    ) -> None:
        err = exc.infrastructure("Database connection lost")

        ack = build_core_exception_ack(err)

        assert ack == {
            "error": {
                "detail": GENERIC_INTERNAL_DETAIL,
                "code": "core.infrastructure",
                "kind": "infrastructure",
            }
        }
        assert "Database connection lost" not in str(ack)

        (row,) = json_records(error_log_buf)
        assert row["level"] == "error"
        assert row["error_kind"] == "infrastructure"
        assert row["detail"] == "Database connection lost"

    def test_internal_with_cause_logged_critical(
        self, error_log_buf: io.StringIO
    ) -> None:
        err = exc.internal("Wiring failed")
        err.__cause__ = RuntimeError("boom")

        ack = build_core_exception_ack(err)

        assert ack["error"]["detail"] == GENERIC_INTERNAL_DETAIL  # type: ignore[index]

        (row,) = json_records(error_log_buf)
        assert row["level"] == "critical"
        assert row["error_kind"] == "internal"


# ....................... #


class TestBuildUnhandledExceptionAck:
    def test_generic_payload_and_critical_log(self, error_log_buf: io.StringIO) -> None:
        ack = build_unhandled_exception_ack(ValueError("sensitive internals"))

        assert ack == {
            "error": {
                "detail": GENERIC_INTERNAL_DETAIL,
                "code": INTERNAL_ERROR_CODE,
                "kind": "internal",
            }
        }
        assert "sensitive internals" not in str(ack)

        (row,) = json_records(error_log_buf)
        assert row["level"] == "critical"
        assert row["event"] == "Unhandled exception"
