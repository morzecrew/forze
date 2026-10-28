"""``forze ops call OP --field VALUE`` — typed flags for an input of flat scalars.

The flags build the same raw object ``--input`` would carry, and it is validated by the same
shared path; only the spelling differs. An input that is not flat scalars, declares a
secret, or has a field spelled like a ``call`` option takes no flags at all.
"""

from __future__ import annotations

import json
from typing import Annotated, Any
from uuid import UUID, uuid4

import pytest
import typer
from pydantic import BaseModel, Field
from typer.testing import CliRunner, Result

from forze_cli.app import app
from forze_cli.ops import build_ops_cli
from forze_cli.ops.flags import CALL_OPTIONS, flag_table
from tests.support import ops_cli_app as sample

# ----------------------- #

runner = CliRunner()

_MODULE = "tests.support.ops_cli_app"


@pytest.fixture(autouse=True)
def _seeded() -> None:
    sample.seed()


def _call(*args: str, app_name: str = "ANONYMOUS") -> Result:
    return runner.invoke(
        app,
        ["ops", "call", *args, "--app", f"{_MODULE}:{app_name}"],
        env={"FORZE_API_KEY": sample.API_KEY, "FORZE_OPS_APP": None},
    )


def _doc(result: Result) -> Any:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, (result.stdout, result.stderr)

    return json.loads(lines[0])


def _error(result: Result, exit_code: int) -> dict[str, Any]:
    assert result.exit_code == exit_code, (result.stdout, result.stderr)

    return _doc(result)["error"]


# ....................... #


class TestTypedFlags:
    def test_each_field_is_a_flag(self) -> None:
        ref = uuid4()

        result = _call(
            "probe.flags",
            "--text",
            "hi",
            "--count",
            "3",
            "--ratio",
            "0.5",
            "--loud",
            "--day",
            "2026-01-02",
            "--ref",
            str(ref),
            "--mode",
            "loud",
            "--level",
            "high",
            "--retry-after",
            "7",
            "--rank",
            "2",
        )

        assert result.exit_code == 0, result.stdout
        assert _doc(result) == {
            "text": "hi",
            "count": 3,
            "ratio": 0.5,
            "loud": True,
            "day": "2026-01-02",
            "ref": str(ref),
            "mode": "loud",
            "level": "high",
            "retry_after": 7,
            "rank": 2,
        }

    def test_an_omitted_flag_takes_the_default(self) -> None:
        document = _doc(_call("probe.flags", "--text", "hi"))

        assert document["count"] == 1
        assert document["loud"] is False
        assert document["ratio"] is None

    def test_equals_form_carries_a_value_that_looks_like_an_option(self) -> None:
        document = _doc(_call("probe.flags", "--text=--pretty", "--count=-2"))

        assert document["text"] == "--pretty"
        assert document["count"] == -2

    def test_a_negative_number_is_a_value(self) -> None:
        assert _doc(_call("probe.flags", "--text", "x", "--count", "-5"))["count"] == -5

    def test_a_switch_turns_off(self) -> None:
        assert _doc(_call("probe.flags", "--text", "x", "--no-loud"))["loud"] is False

    def test_a_flag_spells_the_field_name_not_its_alias(self) -> None:
        # No output type: the model dumps itself, by field name.
        assert _doc(_call("probe.aliased", "--display-name", "Ann")) == {"display_name": "Ann"}

    def test_a_flag_never_reaches_a_field_through_its_alias(self) -> None:
        # first's alias is "second": read by alias, --second would set both fields.
        document = _doc(_call("probe.crossed", "--second", "y"))

        assert document == {"first": "unset", "second": "y"}

    def test_call_options_mix_with_flags(self) -> None:
        result = _call("probe.echo", "--text", "hi", "--pretty")

        assert result.exit_code == 0
        assert result.stdout == '{\n  "text": "hi"\n}\n'

    def test_a_missing_required_field_is_rejected_by_the_dto(self) -> None:
        error = _error(_call("probe.flags", "--count", "2"), 4)

        assert error["code"] == "cli_invalid_input"

    def test_a_bad_value_is_rejected_and_never_echoed(self) -> None:
        result = _call("probe.flags", "--text", "x", "--count", "marker-5be1")

        assert _error(result, 4)["code"] == "cli_invalid_input"
        assert "marker-5be1" not in result.stdout + result.stderr

    def test_the_bound_group_takes_flags(self) -> None:
        root = typer.Typer(pretty_exceptions_show_locals=False)
        root.add_typer(build_ops_cli(sample.ANONYMOUS))

        @root.callback()
        def _root() -> None:  # pyright: ignore[reportUnusedFunction]
            """An application's own CLI."""

        result = runner.invoke(root, ["ops", "call", "probe.echo", "--text", "hi"])

        assert json.loads(result.stdout) == {"text": "hi"}


class TestMalformedFlags:
    @pytest.mark.parametrize(
        "flags",
        [
            ["--text", "x", "--nope", "y"],
            ["--text"],
            ["--text", "x", "--loud=yes"],
            ["--text", "a", "--text", "b"],
            ["--text", "x", "--loud", "--no-loud"],
            ["--text", "x", "stray-marker-81c0"],
            ["--text", "x", "-x"],
            ["--text", "--loud"],
            ["--text", "--count=3"],
            ["--text", "--stray-marker-81c0"],
        ],
    )
    def test_is_usage(self, flags: list[str]) -> None:
        result = _call("probe.flags", *flags)

        assert _error(result, 2)["code"] == "cli_malformed_flags"
        assert "stray-marker-81c0" not in result.stdout + result.stderr

    def test_flags_and_input_are_exclusive(self) -> None:
        result = _call("probe.echo", "--text", "a", "--input", '{"text": "b"}')

        assert _error(result, 2)["code"] == "cli_option_conflict"


class TestWithoutTypedFlags:
    def test_an_input_that_is_not_flat_takes_none(self) -> None:
        error = _error(_call("probe.nested", "--tags", "a"), 2)

        assert error["code"] == "cli_flags_unsupported"
        assert "'tags'" in error["detail"]

    def test_a_misspelt_call_option_is_named(self) -> None:
        nested = _error(_call("probe.nested", "--timout=5"), 2)
        no_input = _error(_call("probe.secure", "--timout", "5"), 2)

        assert "--timout is no call option" in nested["detail"]
        assert "--timout is no call option" in no_input["detail"]

    def test_a_field_spelled_like_a_call_option_takes_none(self) -> None:
        error = _error(_call("probe.collides", "--seconds", "1"), 2)

        assert error["code"] == "cli_flags_unsupported"
        assert "--timeout" in error["detail"]

    def test_a_call_option_keeps_its_meaning_on_such_an_input(self) -> None:
        # --timeout is the call's deadline here, never the field: the field keeps its default.
        assert _doc(_call("probe.collides", "--timeout", "5")) == {"timeout": 1.0}

    def test_an_enum_matched_exactly_takes_none_though_input_takes_it(self) -> None:
        # A plain enum of ints: pydantic never reads "1" as 1, so no flag could set it.
        refused = _error(_call("probe.coded", "--code", "1"), 2)
        accepted = _call("probe.coded", "--input", '{"code": 1}')

        assert refused["code"] == "cli_flags_unsupported"
        assert accepted.exit_code == 0
        assert _doc(accepted) == {"code": 1}

    def test_a_secret_on_argv_is_refused_and_never_echoed(self) -> None:
        result = _call("probe.secret", "--token", "fx-7chr")

        assert _error(result, 2)["code"] == "cli_inline_secret"
        assert "fx-7chr" not in result.stdout + result.stderr

    def test_an_operation_without_input_takes_none(self) -> None:
        assert _error(_call("probe.secure", "--x", "1"), 2)["code"] == "cli_input_not_accepted"


class TestDescribe:
    def _describe(self, op: str, *args: str) -> Result:
        return runner.invoke(
            app,
            ["ops", "describe", op, *args, "--app", f"{_MODULE}:ANONYMOUS"],
            env={"FORZE_OPS_APP": None},
        )

    def test_text_lists_the_flags(self) -> None:
        out = self._describe("probe.flags").stdout

        assert (
            "Typed flags: --text, --count, --ratio, --loud/--no-loud, --day, --ref, --mode, "
            "--level, --retry-after, --rank"
        ) in out

    def test_text_says_why_there_are_none(self) -> None:
        out = self._describe("probe.nested").stdout

        assert "Typed flags: none (field 'tags' is not a scalar)" in out

    def test_json(self) -> None:
        flagged = json.loads(self._describe("probe.echo", "--json").stdout)
        nested = json.loads(self._describe("probe.nested", "--json").stdout)
        secret = json.loads(self._describe("probe.secret", "--json").stdout)

        assert flagged["typed_flags"] == ["--text"]
        assert nested["typed_flags"] is None
        assert secret["typed_flags"] is None


def test_call_options_are_the_reserved_spellings() -> None:
    call = typer.main.get_command(app).commands["ops"].commands["call"]  # type: ignore[attr-defined]
    names = {name for param in call.params for name in param.opts if name.startswith("--")}

    assert names | {"--help"} == CALL_OPTIONS


def test_flags_never_build_a_runtime_before_they_parse() -> None:
    result = _call("probe.flags", "--nope", "x", app_name="BROKEN_RUNTIME")

    assert _error(result, 2)["code"] == "cli_malformed_flags"
    UUID(_doc(result)["execution_id"])


type _Code = Annotated[str, Field(max_length=8)]


class TestFlagTableReadsThroughAliases:
    def test_a_type_alias_of_a_scalar_takes_a_flag(self) -> None:
        class _In(BaseModel):
            code: _Code
            note: _Code | None = None

        table, reason = flag_table(_In)

        assert reason is None
        assert set(table) == {"--code", "--note"}
