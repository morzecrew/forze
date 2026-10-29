"""Unit tests for forze_fastapi.exceptions."""

import io
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone, tzinfo

import pytest
from fastapi import FastAPI
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.base.exceptions import error_envelope, exc
from forze.base.logging import configure_logging
from forze.base.scrubbing import SECRET_PLACEHOLDER
from forze_fastapi.exceptions import (
    ERROR_CODE_HEADER,
    _forze_exception_handler,
    build_core_exception_response,
    register_exception_handlers,
)
from tests.support.logging import reset_forze_stdlib_loggers

# ----------------------- #


def _json_records(stream: io.StringIO) -> list[dict]:
    out: list[dict] = []
    for line in stream.getvalue().strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            out.append(json.loads(line))
    return out


@pytest.fixture
def error_log_buf() -> Iterator[io.StringIO]:
    buf = io.StringIO()
    configure_logging(
        level="info",
        logger_names=["fastapi.errors"],
        stream=buf,
        render_mode="json",
    )
    yield buf
    reset_forze_stdlib_loggers("fastapi.errors")


# ----------------------- #


class TestForzeExceptionHandler:
    @pytest.mark.asyncio
    async def test_not_found_returns_404(self) -> None:
        err = exc.not_found("Document not found")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 404
        assert json.loads(response.body) == {
            "detail": "Document not found",
            "code": "core.not_found",
            "kind": "not_found",
        }
        assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"

    @pytest.mark.asyncio
    async def test_not_found_does_not_log(self, error_log_buf: io.StringIO) -> None:
        err = exc.not_found("Document not found")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        await _forze_exception_handler(request, err)
        assert _json_records(error_log_buf) == []

    @pytest.mark.asyncio
    async def test_conflict_returns_409(self) -> None:
        err = exc.conflict("Revision mismatch")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 409
        assert response.headers.get(ERROR_CODE_HEADER) == "core.conflict"

    @pytest.mark.asyncio
    async def test_validation_returns_422(self) -> None:
        err = exc.validation("Invalid input")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 422
        assert response.headers.get(ERROR_CODE_HEADER) == "core.validation"

    @pytest.mark.asyncio
    async def test_throttled_returns_429(self) -> None:
        err = exc.throttled("Rate limit exceeded", code="rate_limited")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 429
        assert response.headers.get(ERROR_CODE_HEADER) == "rate_limited"

    @pytest.mark.asyncio
    async def test_throttled_exposes_summary_but_not_details(
        self, error_log_buf: io.StringIO
    ) -> None:
        # 429 is a client error: the summary stays visible, but the egress
        # policy hides details (policy names / routes are internal wiring).
        err = exc.throttled("Rate limit exceeded", details={"policy": "p"})
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        body = json.loads(response.body)
        assert body == {
            "detail": "Rate limit exceeded",
            "code": "core.throttled",
            "kind": "throttled",
        }
        assert _json_records(error_log_buf) == []

    @pytest.mark.asyncio
    async def test_unavailable_returns_503_with_summary_and_no_error_log(
        self, error_log_buf: io.StringIO
    ) -> None:
        # An operator switched it off: the client learns so, and nothing is logged as broken.
        err = exc.unavailable("Exports are off", code="exports_off", details={"flag": "f"})
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 503
        assert response.headers.get(ERROR_CODE_HEADER) == "exports_off"
        assert json.loads(response.body) == {
            "detail": "Exports are off",
            "code": "exports_off",
            "kind": "unavailable",
        }
        assert _json_records(error_log_buf) == []

    @pytest.mark.asyncio
    async def test_internal_returns_500(self) -> None:
        err = exc.internal("Something went wrong", code="internal")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 500
        assert response.headers.get(ERROR_CODE_HEADER) == "internal"

    @pytest.mark.asyncio
    async def test_internal_without_cause_logs_error_without_stack(
        self,
        error_log_buf: io.StringIO,
    ) -> None:
        err = exc.internal("Something went wrong", code="internal")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        await _forze_exception_handler(request, err)

        records = _json_records(error_log_buf)
        assert len(records) == 1
        row = records[0]
        assert row["level"] == "error"
        assert row["event"] == "Server error"
        assert row["error_code"] == "internal"
        assert row["error_kind"] == "internal"
        assert "error.stack" not in row

    @pytest.mark.asyncio
    async def test_internal_with_cause_logs_critical_with_stack(
        self,
        error_log_buf: io.StringIO,
    ) -> None:
        try:
            raise ValueError("password=fixture-7chr")
        except ValueError as cause:
            err = exc.internal("Something went wrong", code="internal")
            err.__cause__ = cause

        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        await _forze_exception_handler(request, err)

        records = _json_records(error_log_buf)
        assert len(records) == 1
        row = records[0]
        assert row["level"] == "critical"
        assert row["event"] == "Server error"
        assert row["error_code"] == "internal"
        assert "ValueError" in row["error.stack"]
        assert SECRET_PLACEHOLDER in row["error.message"]
        assert "fixture-7chr" not in row["error.message"]
        assert "fixture-7chr" not in row["error.stack"]

    @pytest.mark.asyncio
    async def test_includes_context_when_error_has_details(self) -> None:
        err = exc.not_found(
            "Document not found",
            details={"table": "users", "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 404
        assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"
        assert json.loads(response.body) == {
            "detail": "Document not found",
            "code": "core.not_found",
            "kind": "not_found",
            "context": {
                "table": "users",
                "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af",
            },
        }

    @pytest.mark.asyncio
    async def test_redacts_sensitive_keys_in_context(self) -> None:
        err = exc.validation(
            "Invalid input",
            details={"password": "fixture-7chr", "field": "email"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 422
        body = json.loads(response.body)
        assert body["context"]["password"] == SECRET_PLACEHOLDER
        assert body["context"]["field"] == "email"

    @pytest.mark.asyncio
    async def test_omits_context_and_summary_on_500(self) -> None:
        err = exc.infrastructure(
            "Database down",
            details={"dsn": "postgres://user:pass@localhost/db"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 500
        assert json.loads(response.body) == {
            "detail": "Internal server error",
            "code": "core.infrastructure",
            "kind": "infrastructure",
        }
        assert b"Database down" not in response.body
        assert b"dsn" not in response.body

    @pytest.mark.asyncio
    async def test_internal_summary_replaced_with_generic_detail(self) -> None:
        err = exc.internal("Dep wiring failed for key X")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 500
        assert json.loads(response.body) == {
            "detail": "Internal server error",
            "code": "core.internal",
            "kind": "internal",
        }
        assert b"wiring" not in response.body

    @pytest.mark.asyncio
    async def test_configuration_kind_leaks_nothing(self) -> None:
        err = exc.configuration(
            "Policy 'secrets-prod' is misconfigured",
            details={"dep_key": "SecretsDepKey", "policy": "secrets-prod"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 500
        assert json.loads(response.body) == {
            "detail": "Internal server error",
            "code": "core.configuration",
            "kind": "configuration",
        }
        assert b"secrets-prod" not in response.body
        assert b"SecretsDepKey" not in response.body

    @pytest.mark.asyncio
    async def test_4xx_kinds_still_expose_summary(self) -> None:
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})

        validation = await _forze_exception_handler(request, exc.validation("Invalid input"))
        assert validation.status_code == 422
        assert json.loads(validation.body) == {
            "detail": "Invalid input",
            "code": "core.validation",
            "kind": "validation",
        }

        authentication = await _forze_exception_handler(
            request, exc.authentication("Token expired")
        )
        assert authentication.status_code == 401
        assert json.loads(authentication.body) == {
            "detail": "Token expired",
            "code": "core.authentication",
            "kind": "authentication",
        }

    @pytest.mark.asyncio
    async def test_body_carries_the_code_a_client_branches_on(self) -> None:
        # A stale revision is a 400 like any domain miss; only the code tells them apart.
        err = exc.precondition("Revision mismatch", code="revision_mismatch")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 400
        assert json.loads(response.body) == {
            "detail": "Revision mismatch",
            "code": "revision_mismatch",
            "kind": "precondition",
        }
        assert response.headers.get(ERROR_CODE_HEADER) == "revision_mismatch"


class TestRegisterExceptionHandlers:
    def test_registers_handler(self) -> None:
        app = FastAPI()

        @app.get("/raise")
        def raise_core_error() -> None:
            raise exc.not_found("Not found")

        register_exception_handlers(app)

        client = TestClient(app)
        response = client.get("/raise")
        assert response.status_code == 404
        assert response.json() == {
            "detail": "Not found",
            "code": "core.not_found",
            "kind": "not_found",
        }
        assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"

    def test_route_raised_server_error_returns_generic_detail(self) -> None:
        app = FastAPI()

        @app.get("/infra")
        def raise_infra() -> None:
            raise exc.infrastructure("Driver said: connection to 10.0.0.5 refused")

        @app.get("/internal")
        def raise_internal() -> None:
            raise exc.internal("Dep wiring failed for key X")

        register_exception_handlers(app)
        client = TestClient(app)

        for path, needle, kind in (
            ("/infra", "10.0.0.5", "infrastructure"),
            ("/internal", "wiring", "internal"),
        ):
            response = client.get(path)
            assert response.status_code == 500
            assert response.json() == {
                "detail": "Internal server error",
                "code": f"core.{kind}",
                "kind": kind,
            }
            assert needle not in response.text

    def test_a_server_kind_body_follows_the_envelope_as_the_header_does(self) -> None:
        # 5xx code and kind are what the envelope gives, as on X-Error-Code and the realtime
        # frames; the summary and details stay masked.
        app = FastAPI()
        err = exc.configuration(
            "Policy 'secrets-prod' is misconfigured",
            code="secrets_policy_invalid",
            details={"policy": "secrets-prod"},
        )

        @app.get("/misconfigured")
        def misconfigured() -> None:
            raise err

        register_exception_handlers(app)
        response = TestClient(app).get("/misconfigured")
        envelope = error_envelope(err)

        assert response.status_code == 500
        assert response.json() == {
            "detail": "Internal server error",
            "code": envelope.code,
            "kind": envelope.kind.value,
        }
        assert response.json()["code"] == response.headers[ERROR_CODE_HEADER]
        assert "secrets-prod" not in response.text

    def test_request_validation_error_uses_forze_envelope(self) -> None:
        from pydantic import BaseModel

        app = FastAPI()

        class Body(BaseModel):
            n: int

        @app.post("/echo")
        def echo(body: Body) -> dict:  # type: ignore[type-arg]
            return {"n": body.n}

        register_exception_handlers(app)
        client = TestClient(app)

        response = client.post("/echo", json={"n": "not-an-int"})

        # Rendered in the shared envelope (not FastAPI's default 422 shape).
        assert response.status_code == 422
        assert response.headers.get(ERROR_CODE_HEADER) == "request_validation_error"
        body = response.json()
        assert body["detail"] == "Request validation failed"
        errors = body["context"]["errors"]
        assert any(e["loc"][-1] == "n" for e in errors)
        # Only JSON-safe loc/msg/type kept; raw ctx/input dropped.
        assert all("ctx" not in e and "input" not in e for e in errors)

    def test_unhandled_exception_returns_500_json(
        self,
        error_log_buf: io.StringIO,
    ) -> None:
        app = FastAPI()

        @app.get("/raise")
        def raise_unhandled() -> None:
            raise ValueError("Something broke")

        register_exception_handlers(app)

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/raise")
        assert response.status_code == 500
        # The envelope's own code for an unhandled error: nothing of the exception reaches it.
        assert response.json() == {
            "detail": "Internal server error",
            "code": "core.internal",
            "kind": "internal",
        }
        assert response.headers.get(ERROR_CODE_HEADER) == "core.internal"

        records = _json_records(error_log_buf)
        assert len(records) == 1
        row = records[0]
        assert row["level"] == "critical"
        assert row["event"] == "Unhandled exception"
        assert row["error.type"] == "ValueError"
        assert "ValueError" in row["error.stack"]


class TestRetryAfterHeader:
    def test_retry_after_seconds_become_the_header(self) -> None:
        err = exc.throttled(
            "Rate limit exceeded", details={"retry_after": 30, "policy": "internal-policy"}
        )

        response = build_core_exception_response(err)

        assert response.status_code == 429
        assert response.headers["Retry-After"] == "30"
        # The rest of the details stay withheld.
        assert "context" not in json.loads(response.body)
        assert b"internal-policy" not in response.body

    def test_fractional_seconds_round_up(self) -> None:
        # Rounding down would send the client back before the limit has reset.
        err = exc.throttled("Slow down", details={"retry_after": 2.1})

        assert build_core_exception_response(err).headers["Retry-After"] == "3"

    def test_retry_at_becomes_an_http_date(self) -> None:
        moscow = timezone(timedelta(hours=3))
        at = datetime(2026, 10, 21, 10, 28, tzinfo=moscow)
        err = exc.throttled("Quota spent", details={"retry_at": at})

        header = build_core_exception_response(err).headers["Retry-After"]

        assert header == "Wed, 21 Oct 2026 07:28:00 GMT"

    def test_retry_at_rounds_up_to_the_next_second(self) -> None:
        # The HTTP-date drops sub-seconds; truncating would send the client back early.
        at = datetime(2026, 10, 21, 7, 28, 0, 500_000, tzinfo=UTC)
        err = exc.throttled("Quota spent", details={"retry_at": at})

        header = build_core_exception_response(err).headers["Retry-After"]

        assert header == "Wed, 21 Oct 2026 07:28:01 GMT"

    def test_retry_after_wins_over_retry_at(self) -> None:
        at = datetime(2026, 10, 21, 7, 28, tzinfo=UTC)
        err = exc.throttled("Slow down", details={"retry_after": 5, "retry_at": at})

        assert build_core_exception_response(err).headers["Retry-After"] == "5"

    @pytest.mark.parametrize(
        "details",
        [
            {},
            {"retry_after": -1},
            {"retry_after": float("nan")},
            {"retry_after": float("inf")},
            {"retry_after": "30"},
            {"retry_after": True},
            {"retry_at": datetime(2026, 10, 21, 7, 28)},
            {"retry_at": "2026-10-21T07:28:00+00:00"},
            {"retry_at": datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1)))},
            # A zone whose utcoffset raises (a tzinfo subclass that does not implement it).
            {"retry_at": datetime(2026, 10, 21, 7, 28, tzinfo=tzinfo())},
            # Rounding up to the next second passes the last instant a datetime holds.
            {"retry_at": datetime.max.replace(tzinfo=UTC)},
        ],
        ids=[
            "none",
            "negative",
            "nan",
            "infinite",
            "string-seconds",
            "bool",
            "naive-datetime",
            "string-instant",
            "out-of-range",
            "zone-raises",
            "rounds-past-max",
        ],
    )
    def test_an_unusable_hint_sends_no_header(self, details: dict[str, object]) -> None:
        response = build_core_exception_response(exc.throttled("Slow down", details=details))

        assert response.status_code == 429
        assert "Retry-After" not in response.headers

    def test_an_unavailable_error_sends_it(self) -> None:
        err = exc.unavailable("Switched off for maintenance", details={"retry_after": 120})

        response = build_core_exception_response(err)

        assert response.status_code == 503
        assert response.headers["Retry-After"] == "120"

    def test_only_a_throttled_or_unavailable_error_sends_it(self) -> None:
        err = exc.infrastructure("Upstream busy", details={"retry_after": 30})

        response = build_core_exception_response(err)

        assert response.status_code == 500
        assert "Retry-After" not in response.headers

    def test_a_route_raised_throttle_answers_with_the_header(self) -> None:
        app = FastAPI()

        @app.get("/limited")
        def limited() -> None:
            raise exc.throttled("Slow down", code="rate_limited", details={"retry_after": 7})

        register_exception_handlers(app)
        response = TestClient(app).get("/limited")

        assert response.status_code == 429
        assert response.headers["Retry-After"] == "7"
        assert response.json()["code"] == "rate_limited"
