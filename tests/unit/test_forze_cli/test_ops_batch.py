"""``forze ops call OP --batch @PATH|-`` — one call per JSONL line, in one runtime scope.

Every line is read and validated before the runtime is built, so a bad line runs nothing.
Each call then writes one JSON line, numbered by its input line, and the batch stops at the
first failure, which sets the exit code.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from typer.testing import CliRunner, Result

from forze_cli.app import app
from tests.support import ops_cli_app as sample

# ----------------------- #

runner = CliRunner()

_MODULE = "tests.support.ops_cli_app"


@pytest.fixture(autouse=True)
def _seeded() -> None:
    sample.seed()


def _batch(*args: str, stdin: str, source: str = "-", app_name: str = "API") -> Result:
    return runner.invoke(
        app,
        ["ops", "call", *args, "--batch", source, "--app", f"{_MODULE}:{app_name}"],
        env={"FORZE_API_KEY": sample.API_KEY, "FORZE_OPS_APP": None},
        input=stdin,
    )


def _jsonl(*lines: Any) -> str:
    return "".join((line if isinstance(line, str) else json.dumps(line)) + "\n" for line in lines)


def _documents(result: Result) -> list[dict[str, Any]]:
    return [json.loads(line) for line in result.stdout.splitlines()]


# ....................... #


class TestBatch:
    def test_each_line_is_one_call_in_one_scope(self) -> None:
        result = _batch("notes.add", stdin=_jsonl({"text": "a"}, "", {"text": "b"}))

        assert result.exit_code == 0, result.stdout
        documents = _documents(result)
        assert [(d["line"], d["result"]) for d in documents] == [
            (1, {"count": 1}),
            (3, {"count": 2}),
        ]
        assert sample.ADDED == ["a", "b"]
        assert sample.SHUTDOWNS == ["ran"]

    def test_lines_share_a_correlation_id_and_not_an_execution_id(self) -> None:
        correlation = uuid4()
        given = _documents(
            _batch(
                "probe.echo",
                "--correlation-id",
                str(correlation),
                stdin=_jsonl(*[{"text": "x"}] * 2),
            )
        )
        minted = _documents(_batch("probe.echo", stdin=_jsonl(*[{"text": "x"}] * 2)))

        assert {d["correlation_id"] for d in given} == {str(correlation)}
        assert len({d["correlation_id"] for d in minted}) == 1
        assert len({d["execution_id"] for d in given + minted}) == 4

    def test_each_line_binds_afresh_and_leaves_nothing_bound(self) -> None:
        result = _batch("probe.bound_input", "--timeout", "30", stdin=_jsonl(*[{"text": "x"}] * 2))

        assert result.exit_code == 0, result.stdout
        documents = _documents(result)
        assert [d["result"]["execution_id"] for d in documents] == [
            d["execution_id"] for d in documents
        ]
        assert documents[0]["execution_id"] != documents[1]["execution_id"]

        for document in documents:
            bound = document["result"]
            assert bound["principal_id"] == str(sample.OPERATOR)
            assert bound["idempotency_key"] is None
            assert bound["deadline_set"] is True

        # After the last line: no metadata, identity, tenant, key or deadline left behind.
        assert sample.LEFT_BOUND == [(None, None, None, None, None)]

    def test_a_key_revoked_between_lines_refuses_the_next_line(self) -> None:
        result = _batch("probe.revoke", stdin=_jsonl({"text": "a"}, {"text": "b"}))

        assert result.exit_code == 5
        documents = _documents(result)
        assert documents[0] == {**documents[0], "line": 1, "result": None}
        assert documents[1]["line"] == 2
        assert documents[1]["error"]["kind"] == "authentication"

    def test_the_first_failure_stops_the_batch_and_sets_the_exit_code(self) -> None:
        result = _batch("probe.fail", stdin=_jsonl({}, {"kind": "domain"}, {}))

        assert result.exit_code == 4
        documents = _documents(result)
        assert [d["line"] for d in documents] == [1, 2]
        assert documents[0]["result"] is None
        assert documents[1]["error"]["kind"] == "domain"
        UUID(documents[1]["execution_id"])

    def test_a_retryable_failure_exits_75_from_queries_and_69_from_commands(self) -> None:
        line = _jsonl({"kind": "infrastructure"})

        assert _batch("probe.fail", stdin=line).exit_code == 75
        assert _batch("probe.fail_write", stdin=line).exit_code == 69

    def test_only_a_newline_ends_a_line(self) -> None:
        # JSON allows a raw U+2028 or U+0085 in a string; str.splitlines() splits at both.
        stdin = '{"text": "a\u2028b"}\r\n{"text": "c\x85d"}\n'

        result = _batch("probe.echo", stdin=stdin)

        assert result.exit_code == 0, result.stdout
        assert [(d["line"], d["result"]["text"]) for d in _documents(result)] == [
            (1, "a\u2028b"),
            (2, "c\x85d"),
        ]

    def test_an_empty_batch_runs_nothing(self) -> None:
        result = _batch("notes.add", stdin="\n")

        assert result.exit_code == 0
        assert result.stdout == ""
        assert sample.SHUTDOWNS == []

    def test_from_a_file(self, tmp_path: Path) -> None:
        source = tmp_path / "lines.jsonl"
        source.write_text(_jsonl({"text": "a"}))

        result = _batch("notes.add", stdin="", source=f"@{source}")

        assert _documents(result)[0]["result"] == {"count": 1}

    def test_a_secret_is_fine_off_argv(self) -> None:
        result = _batch("probe.secret", stdin=_jsonl({"token": "fx-7chr"}))

        assert _documents(result)[0]["result"] == {"text": "7 characters"}


class TestRefusedBeforeAnythingRuns:
    def test_a_malformed_line(self) -> None:
        result = _batch("notes.add", stdin=_jsonl({"text": "a"}, "{not json", {"text": "c"}))

        assert result.exit_code == 2
        [document] = _documents(result)
        assert document["line"] == 2
        assert document["error"]["code"] == "cli_malformed_input"
        assert sample.ADDED == []
        assert sample.SHUTDOWNS == []

    def test_an_invalid_line(self) -> None:
        result = _batch("notes.add", stdin=_jsonl({"text": "a"}, {"txet": "b"}))

        assert result.exit_code == 4
        [document] = _documents(result)
        assert document["line"] == 2
        assert document["error"]["code"] == "cli_invalid_input"
        assert sample.ADDED == []

    @pytest.mark.parametrize(
        "option",
        [["--input", "{}"], ["--idempotency-key", "job-1"], ["--pretty"], ["--text", "x"]],
    )
    def test_an_option_a_batch_cannot_take(self, option: list[str]) -> None:
        result = _batch("notes.add", *option, stdin=_jsonl({"text": "a"}))

        assert result.exit_code == 2
        [document] = _documents(result)
        assert "line" not in document
        assert document["error"]["code"] == "cli_option_conflict"
        assert sample.ADDED == []

    def test_an_operation_without_input(self) -> None:
        result = _batch("probe.bound", stdin=_jsonl({}))

        assert _documents(result)[0]["error"]["code"] == "cli_input_not_accepted"

    def test_an_unreadable_file(self, tmp_path: Path) -> None:
        result = _batch("notes.add", stdin="", source=f"@{tmp_path / 'missing.jsonl'}")

        assert result.exit_code == 2
        assert _documents(result)[0]["error"]["code"] == "cli_input_unreadable"

    def test_a_source_that_is_not_a_file_or_stdin_is_click_usage(self) -> None:
        result = _batch("notes.add", stdin="", source="lines.jsonl")

        assert result.exit_code == 2
        assert result.stdout == ""
        assert "Usage:" in result.stderr
