"""Proves the ``forze ops`` example: listed, described, and called with a verified key."""

from __future__ import annotations

import json

from typer.testing import CliRunner

from forze_cli.app import app

# ----------------------- #

runner = CliRunner()

_APP = "examples.recipes.ops_cli.app:ops"


def _ops(*args: str, key: str | None = "demo-operator-key") -> tuple[int, str]:
    result = runner.invoke(
        app, ["ops", *args, "--app", _APP], env={"FORZE_API_KEY": key, "FORZE_OPS_APP": None}
    )

    return result.exit_code, result.stdout


class TestOpsCliExample:
    def test_lists_only_the_allowlist(self) -> None:
        code, out = _ops("list", "--json")

        assert code == 0
        assert [item["op"] for item in json.loads(out)] == [
            "notes.create",
            "notes.get",
            "notes.list",
        ]

    def test_describes_an_operation(self) -> None:
        code, out = _ops("describe", "notes.create")

        assert code == 0
        assert "notes.create (command)" in out

    def test_creates_and_reads_back_with_a_verified_key(self) -> None:
        code, out = _ops("call", "notes.create", "--input", '{"title": "hi"}')

        assert code == 0
        created = json.loads(out)

        code, out = _ops("call", "notes.get", "--input", json.dumps({"id": created["id"]}))

        assert code == 0
        assert json.loads(out)["title"] == "hi"

    def test_creates_with_typed_flags(self) -> None:
        code, out = _ops("call", "notes.create", "--title", "hi")

        assert code == 0
        assert json.loads(out)["title"] == "hi"

    def test_an_unknown_key_is_denied(self) -> None:
        code, out = _ops("call", "notes.list", key="not-a-key")

        assert code == 5
        assert json.loads(out)["error"]["kind"] == "authentication"
