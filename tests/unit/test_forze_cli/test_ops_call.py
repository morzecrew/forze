"""``forze ops call`` — one governed invocation, one stdout document, one exit code.

Drives the sample application in :mod:`tests.support.ops_cli_app` by import string through
the root CLI, as an operator would, and asserts the machine contract: the document's shape,
the exit-code table, the envelope options and the identity choices.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from typer.testing import CliRunner, Result

from forze.base.exceptions import ExceptionKind, exception_egress_policy
from forze_cli.app import app
from forze_cli.ops.contract import KIND_EXIT_CODES
from tests.support import ops_cli_app as sample

# ----------------------- #

runner = CliRunner()

_MODULE = "tests.support.ops_cli_app"


@pytest.fixture(autouse=True)
def _seeded() -> None:
    sample.seed()


def _call(
    *args: str,
    app_name: str = "API",
    key: str | None = sample.API_KEY,
    stdin: str | None = None,
) -> Result:
    return runner.invoke(
        app,
        ["ops", "call", *args, "--app", f"{_MODULE}:{app_name}"],
        env={"FORZE_API_KEY": key, "FORZE_OPS_APP": None},
        input=stdin,
    )


def _doc(result: Result) -> Any:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.stdout

    return json.loads(lines[0])


def _error(result: Result, exit_code: int) -> dict[str, Any]:
    assert result.exit_code == exit_code, (result.stdout, result.stderr)
    document = _doc(result)
    UUID(document["execution_id"])
    UUID(document["correlation_id"])

    return document["error"]


# ....................... #


class TestResult:
    def test_the_result_is_the_one_document(self) -> None:
        result = _call("probe.echo", "--input", '{"text": "hi"}')

        assert result.exit_code == 0
        assert _doc(result) == {"text": "hi"}

    def test_a_void_operation_writes_null(self) -> None:
        result = _call("probe.secure")

        assert result.exit_code == 0
        assert result.stdout == "null\n"

    def test_pretty_indents(self) -> None:
        result = _call("probe.echo", "--input", '{"text": "hi"}', "--pretty")

        assert result.stdout == '{\n  "text": "hi"\n}\n'


class TestInput:
    def test_from_a_file(self, tmp_path: Path) -> None:
        source = tmp_path / "input.json"
        source.write_text('{"text": "from a file"}')

        assert _doc(_call("probe.echo", "--input", f"@{source}")) == {"text": "from a file"}

    def test_from_stdin(self) -> None:
        result = _call("probe.echo", "--input", "-", stdin='{"text": "piped"}')

        assert _doc(result) == {"text": "piped"}

    def test_an_unreadable_file_is_usage(self, tmp_path: Path) -> None:
        error = _error(_call("probe.echo", "--input", f"@{tmp_path / 'missing.json'}"), 2)

        assert error["code"] == "cli_input_unreadable"

    @pytest.mark.parametrize("raw", ["{not json", "[1, 2]", '{"text": NaN}', '{"n": Infinity}'])
    def test_malformed_json_is_usage_and_never_echoed(self, raw: str) -> None:
        result = _call("probe.echo", "--input", raw)

        assert _error(result, 2)["code"] == "cli_malformed_input"
        assert raw not in result.stdout + result.stderr

    def test_input_to_an_operation_without_any_is_usage(self) -> None:
        assert _error(_call("probe.bound", "--input", "{}"), 2)["code"] == "cli_input_not_accepted"

    def test_an_inline_secret_is_refused_and_stdin_accepted(self) -> None:
        inline = _call("probe.secret", "--input", '{"token": "fx-7chr"}')

        assert _error(inline, 2)["code"] == "cli_inline_secret"
        assert "fx-7chr" not in inline.stdout + inline.stderr

        piped = _call("probe.secret", "--input", "-", stdin='{"token": "fx-7chr"}')

        assert _doc(piped) == {"text": "7 characters"}

    def test_an_unknown_key_is_rejected(self) -> None:
        error = _error(_call("probe.echo", "--input", '{"text": "x", "txet": "y"}'), 4)

        assert error["code"] == "cli_invalid_input"
        assert error["context"] == {"unknown": ["txet"]}

    def test_a_value_in_the_wrong_field_is_never_echoed(self) -> None:
        result = _call("probe.echo", "--input", '{"text": {"misplaced": "echo-marker-7f3a"}}')

        assert _error(result, 4)["code"] == "cli_invalid_input"
        assert "echo-marker-7f3a" not in result.stdout + result.stderr


class TestExposure:
    def test_an_unexposed_operation_reads_as_unknown(self) -> None:
        # A command is not exposed without include_writes, whatever the registry holds.
        error = _error(_call("notes.add", "--input", '{"text": "x"}', app_name="ANONYMOUS"), 2)

        assert error["code"] == "cli_unknown_operation"

    def test_an_unknown_operation_is_usage(self) -> None:
        assert _error(_call("nope.nothing"), 2)["code"] == "cli_unknown_operation"


class TestIdentity:
    def test_an_environment_key_binds_its_principal(self) -> None:
        bound = _doc(_call("probe.bound"))

        assert bound["principal_id"] == str(sample.OPERATOR)
        assert bound["tenant_id"] is None

    def test_a_key_file_binds_its_principal(self, tmp_path: Path) -> None:
        key_file = tmp_path / "key"
        key_file.write_text(sample.MEMBER_KEY + "\n")

        bound = _doc(_call("probe.bound", "--api-key-file", str(key_file), key=None))

        assert bound["principal_id"] == str(sample.MEMBER)

    def test_the_principal_tenant_resolves(self) -> None:
        bound = _doc(_call("probe.bound", key=sample.MEMBER_KEY))

        assert bound["tenant_id"] == str(sample.TENANT)

    def test_an_invalid_key_is_denied_before_the_operation_runs(self) -> None:
        error = _error(_call("notes.add", "--input", '{"text": "x"}', key="wrong"), 5)

        assert error["kind"] == "authentication"
        assert sample.ADDED == []

    def test_no_key_is_denied(self) -> None:
        assert _error(_call("probe.bound", key=None), 5)["code"] == "cli_api_key_missing"

    def test_no_option_takes_a_key_on_argv(self) -> None:
        result = runner.invoke(app, ["ops", "call", "--help"])

        assert "--api-key " not in result.stdout
        assert "--api-key-file" in result.stdout

    def test_anonymous_fails_an_authn_requirement(self) -> None:
        error = _error(_call("probe.secure", app_name="ANONYMOUS"), 5)

        assert error["code"] == "auth_required"

    def test_static_binds_its_fixed_identity(self) -> None:
        bound = _doc(_call("probe.bound", app_name="STATIC", key=None))

        assert bound["principal_id"] == str(sample.OPERATOR)
        assert bound["tenant_id"] == str(sample.TENANT)

    @pytest.mark.parametrize("app_name", ["STATIC", "ANONYMOUS"])
    def test_a_tenant_without_a_key_is_usage(self, app_name: str) -> None:
        error = _error(_call("probe.bound", "--tenant", str(uuid4()), app_name=app_name), 2)

        assert error["code"] == "cli_tenant_unverifiable"

    def test_a_tenant_without_a_resolver_is_usage(self) -> None:
        result = _call("probe.bound", "--tenant", str(uuid4()), app_name="API_NO_TENANCY")

        assert _error(result, 2)["code"] == "tenant_request_unverifiable"

    def test_a_tenant_the_key_carries_is_bound_without_a_resolver(self) -> None:
        result = _call(
            "probe.bound",
            "--tenant",
            str(sample.TENANT),
            app_name="API_NO_TENANCY",
            key=sample.ISSUER_KEY,
        )

        assert _doc(result)["tenant_id"] == str(sample.TENANT)

    def test_a_tenant_outside_membership_is_denied(self) -> None:
        result = _call("probe.bound", "--tenant", str(uuid4()), key=sample.MEMBER_KEY)

        assert _error(result, 5)["code"] == "tenant_mismatch"

    def test_a_member_tenant_is_bound(self) -> None:
        result = _call("probe.bound", "--tenant", str(sample.TENANT), key=sample.MEMBER_KEY)

        assert _doc(result)["tenant_id"] == str(sample.TENANT)

    def test_no_tenant_fails_a_tenant_requirement(self) -> None:
        assert _error(_call("probe.tenanted"), 5)["code"] == "tenant_required"

    def test_an_api_key_file_without_a_key_identity_is_usage(self, tmp_path: Path) -> None:
        key_file = tmp_path / "key"
        key_file.write_text("x")

        result = _call("probe.bound", "--api-key-file", str(key_file), app_name="STATIC")

        assert _error(result, 2)["code"] == "cli_api_key_file_unused"

    def test_an_empty_key_file_is_denied_naming_the_file(self, tmp_path: Path) -> None:
        key_file = tmp_path / "key"
        key_file.write_text("\n")

        error = _error(_call("probe.bound", "--api-key-file", str(key_file), key=None), 5)

        assert error["code"] == "cli_api_key_missing"
        assert "is empty" in error["detail"]


class TestEnvelope:
    def test_metadata_reaches_the_operation(self) -> None:
        correlation, causation = uuid4(), uuid4()

        bound = _doc(
            _call(
                "probe.bound",
                "--correlation-id",
                str(correlation),
                "--causation-id",
                str(causation),
                "--timeout",
                "30",
            )
        )

        assert bound["correlation_id"] == str(correlation)
        assert bound["causation_id"] == str(causation)
        assert bound["deadline_set"] is True

    def test_the_error_object_echoes_the_correlation_id(self) -> None:
        correlation = uuid4()

        result = _call("probe.bound", "--correlation-id", str(correlation), key="wrong")

        assert _doc(result)["correlation_id"] == str(correlation)

    def test_an_idempotency_key_needs_support(self) -> None:
        result = _call("probe.slow", "--input", '{"seconds": 0}', "--idempotency-key", "k")

        assert _error(result, 2)["code"] == "cli_idempotency_unsupported"

    def test_a_repeated_key_replays_and_a_changed_input_conflicts(self) -> None:
        first = _call("notes.add", "--input", '{"text": "a"}', "--idempotency-key", "job-1")
        again = _call("notes.add", "--input", '{"text": "a"}', "--idempotency-key", "job-1")

        assert _doc(first) == _doc(again) == {"count": 1}
        assert sample.ADDED == ["a"]

        changed = _call("notes.add", "--input", '{"text": "b"}', "--idempotency-key", "job-1")

        assert _error(changed, 4)["kind"] == "conflict"

    def test_a_timeout_exits_6(self) -> None:
        result = _call("probe.slow", "--input", '{"seconds": 5}', "--timeout", "0.05")

        assert _error(result, 6)["code"] == "deadline_exceeded"

    @pytest.mark.parametrize(
        "option",
        [
            ["--timeout", "0"],
            ["--timeout", "-1"],
            ["--grace", "-1"],
            ["--grace", "nan"],
            ["--grace", "inf"],
            ["--timeout", "nan"],
            ["--timeout", "inf"],
            ["--correlation-id", "not-a-uuid"],
            ["--tenant", "not-a-uuid"],
        ],
    )
    def test_a_malformed_option_is_click_usage(self, option: list[str]) -> None:
        result = _call("probe.bound", *option)

        assert result.exit_code == 2
        assert result.stdout == ""
        assert "Usage:" in result.stderr


class TestExitCodes:
    def test_every_kind_is_retryable_or_mapped(self) -> None:
        unmapped = [
            kind
            for kind in ExceptionKind
            if not exception_egress_policy(kind).retryable and kind not in KIND_EXIT_CODES
        ]

        assert unmapped == []

    @pytest.mark.parametrize("kind", list(ExceptionKind))
    def test_a_query_failure_exits_by_kind(self, kind: ExceptionKind) -> None:
        expected = 75 if exception_egress_policy(kind).retryable else KIND_EXIT_CODES[kind]

        error = _error(_call("probe.fail", "--input", json.dumps({"kind": kind})), expected)

        assert error["kind"] == kind

    def test_a_retryable_command_failure_exits_75_with_a_key(self) -> None:
        result = _call(
            "probe.fail_keyed", "--input", '{"kind": "infrastructure"}', "--idempotency-key", "k"
        )

        assert _error(result, 75)["retryable"] is True

    def test_a_retryable_command_failure_exits_69_without_a_key(self) -> None:
        result = _call("probe.fail_write", "--input", '{"kind": "infrastructure"}')

        assert _error(result, 69)["retryable"] is True

    def test_a_server_error_is_masked(self) -> None:
        error = _error(_call("probe.fail", "--input", '{"kind": "internal"}'), 1)

        assert error["detail"] == "Internal server error"

    def test_a_covered_denial_collapses_to_not_found(self) -> None:
        covered = _call(
            "probe.deny", "--input", '{"resource_type": "notes"}', app_name="NON_DISCLOSING"
        )
        uncovered = _call(
            "probe.deny", "--input", '{"resource_type": "orders"}', app_name="NON_DISCLOSING"
        )

        assert _error(covered, 3)["kind"] == "not_found"
        assert _error(uncovered, 5)["kind"] == "authorization"


class TestRuntime:
    def test_a_runtime_that_is_not_serverless_is_configuration(self) -> None:
        error = _error(_call("probe.bound", app_name="NOT_SERVERLESS"), 78)

        assert error["code"] == "cli_runtime_not_serverless"

    def test_a_cancellation_the_cli_did_not_request_is_still_one_document(self) -> None:
        assert _error(_call("probe.cancelled", app_name="ANONYMOUS"), 1)["code"] == "core.internal"

    def test_an_unhandled_failure_is_still_one_document(self) -> None:
        result = _call("probe.bound", app_name="BROKEN_RUNTIME")

        assert _error(result, 1)["code"] == "core.internal"
        assert "factory bug" not in result.stdout

    def test_shutdown_runs_after_success_and_after_failure(self) -> None:
        assert _call("probe.bound").exit_code == 0
        assert _call("probe.fail", "--input", '{"kind": "domain"}').exit_code == 4

        assert sample.SHUTDOWNS == ["ran", "ran"]
