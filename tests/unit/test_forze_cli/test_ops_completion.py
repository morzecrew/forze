"""Shell completion of ``OP`` for ``forze ops describe`` and ``call``.

Completion runs the CLI with the shell's words in the environment and reads candidates from
stdout, so it loads the declaration as ``list`` does, never builds a runtime, and completes
nothing, silently, when the declaration cannot load.
"""

from __future__ import annotations

import pytest
import typer
from typer.testing import CliRunner

from forze_cli.app import app
from forze_cli.ops import build_ops_cli
from tests.support import ops_cli_app as sample

# ----------------------- #

runner = CliRunner()

_MODULE = "tests.support.ops_cli_app"


def _complete(
    words: str, *, env_app: str | None = None, cli: typer.Typer = app, prog: str = "forze"
) -> list[str]:
    result = runner.invoke(
        cli,
        [],
        prog_name=prog,
        env={
            f"_{prog.upper()}_COMPLETE": "complete_bash",
            "COMP_WORDS": words,
            "COMP_CWORD": str(len(words.split(" ")) - 1),
            "FORZE_OPS_APP": env_app,
        },
    )

    assert result.exit_code == 0, result.output
    assert result.stderr == ""

    return result.stdout.split()


# ....................... #


class TestCompletion:
    def test_call_completes_exposed_operations(self) -> None:
        candidates = _complete("forze ops call probe.e", env_app=f"{_MODULE}:ANONYMOUS")

        assert candidates == ["probe.echo"]

    def test_describe_completes_every_exposed_operation(self) -> None:
        candidates = _complete("forze ops describe ", env_app=f"{_MODULE}:ANONYMOUS")

        assert candidates == sorted(sample.ANONYMOUS.exposed)
        assert "notes.add" not in candidates

    def test_app_on_the_line_names_the_declaration(self) -> None:
        candidates = _complete(f"forze ops call --app {_MODULE}:API notes")

        assert candidates == ["notes.add"]

    def test_output_while_loading_is_no_candidate(self) -> None:
        assert _complete("forze ops call probe.e", env_app=f"{_MODULE}:chatty") == ["probe.echo"]

    def test_completion_never_builds_a_runtime(self) -> None:
        assert _complete("forze ops call probe.e", env_app=f"{_MODULE}:BROKEN_RUNTIME") == [
            "probe.echo"
        ]

    @pytest.mark.parametrize(
        "env_app",
        [
            None,
            "no-colon",
            f"{_MODULE}_missing:APP",
            f"{_MODULE}:unbuildable",
            f"{_MODULE}:NOT_AN_OPS_APP",
        ],
    )
    def test_a_declaration_that_cannot_load_completes_nothing_quietly(
        self, env_app: str | None
    ) -> None:
        assert _complete("forze ops call ", env_app=env_app) == []

    def test_the_bound_group_completes_from_its_declaration(self) -> None:
        root = typer.Typer(pretty_exceptions_show_locals=False)
        root.add_typer(build_ops_cli(sample.ANONYMOUS))

        @root.callback()
        def _root() -> None:  # pyright: ignore[reportUnusedFunction]
            """An application's own CLI."""

        assert _complete("myapp ops call probe.e", cli=root, prog="myapp") == ["probe.echo"]
