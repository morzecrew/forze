"""``forze ops list`` / ``describe``, finding the declaration, and embedding the group.

Also the declaration itself: exposure is checked when it is built, and the identity choice
is explicit, with a static identity refused unless acknowledged.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from forze.application.contracts.authn import AuthnIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_cli.app import app
from forze_cli.ops import AnonymousIdentity, OpsApp, StaticIdentity, build_ops_cli
from tests.support import ops_cli_app as sample

# ----------------------- #

runner = CliRunner()

_MODULE = "tests.support.ops_cli_app"
_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def _seeded() -> None:
    sample.seed()


def _ops(*args: str, app_name: str | None = "ANONYMOUS") -> typer.testing.Result:
    target = ["--app", f"{_MODULE}:{app_name}"] if app_name else []

    return runner.invoke(app, ["ops", *args, *target], env={"FORZE_OPS_APP": None})


# ....................... #


class TestDeclaration:
    def test_queries_only_by_default(self) -> None:
        catalog = sample.REGISTRY.catalog()

        assert sample.ANONYMOUS.exposed
        assert all(catalog[op].is_read_only for op in sample.ANONYMOUS.exposed.values())

    def test_include_writes_adds_commands(self) -> None:
        assert "notes.add" in sample.API.exposed

    def test_a_single_key_allowlist_is_one_operation(self) -> None:
        ops = OpsApp(
            registry=sample.REGISTRY,
            runtime=sample.runtime,
            identity=AnonymousIdentity(),
            operations="probe.echo",
        )

        assert list(ops.exposed) == ["probe.echo"]

    @pytest.mark.parametrize(
        ("operations", "include_writes", "fragment"),
        [
            ([], False, "at least one operation"),
            (["probe.nothing"], False, "Unknown operations"),
            (["notes.add"], False, "need include_writes=True"),
        ],
    )
    def test_an_allowlist_is_checked_like_a_grant(
        self, operations: list[str], include_writes: bool, fragment: str
    ) -> None:
        with pytest.raises(CoreException) as caught:
            OpsApp(
                registry=sample.REGISTRY,
                runtime=sample.runtime,
                identity=AnonymousIdentity(),
                include_writes=include_writes,
                operations=operations,
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert fragment in caught.value.summary

    def test_a_sensitive_operation_is_refused_at_declaration(self) -> None:
        with pytest.raises(CoreException) as caught:
            OpsApp(
                registry=sample.build_registry(sensitive=True),
                runtime=sample.runtime,
                identity=AnonymousIdentity(),
            )

        assert "sensitive" in caught.value.summary

    @pytest.mark.parametrize("acknowledged", [None, False])
    def test_a_static_identity_needs_its_acknowledgement(self, acknowledged: bool | None) -> None:
        kwargs = {} if acknowledged is None else {"acknowledge_unverified_identity": acknowledged}

        with pytest.raises(CoreException) as caught:
            StaticIdentity(authn=AuthnIdentity(principal_id=sample.OPERATOR), **kwargs)

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    def test_the_identity_choice_has_no_default(self) -> None:
        with pytest.raises(TypeError):
            OpsApp(registry=sample.REGISTRY, runtime=sample.runtime)  # type: ignore[call-arg]

        with pytest.raises(TypeError):
            OpsApp(registry=sample.REGISTRY, runtime=sample.runtime, identity="operator")  # type: ignore[arg-type]

    def test_the_declaration_imports_without_typer(self) -> None:
        code = (
            "import sys; sys.modules['typer'] = None\n"
            f"import {_MODULE} as m\n"
            "assert 'probe.echo' in m.ANONYMOUS.exposed\n"
        )

        result = subprocess.run(
            [sys.executable, "-c", code], cwd=_ROOT, capture_output=True, text=True, check=False
        )

        assert result.returncode == 0, result.stderr


# ....................... #


class TestList:
    def test_text_lists_exposed_operations(self) -> None:
        result = _ops("list")

        assert result.exit_code == 0
        assert "probe.echo" in result.stdout
        assert "Echo the text." in result.stdout
        assert "Second line" not in result.stdout
        assert "notes.add" not in result.stdout

    def test_json(self) -> None:
        listing = json.loads(_ops("list", "--json").stdout)

        assert {"op": "probe.bound", "kind": "query", "title": "What a call binds"} in listing

    def test_list_and_describe_never_build_a_runtime(self) -> None:
        assert _ops("list", app_name="BROKEN_RUNTIME").exit_code == 0
        assert _ops("describe", "probe.echo", app_name="BROKEN_RUNTIME").exit_code == 0


class TestDescribe:
    def test_text(self) -> None:
        result = _ops("describe", "probe.secure")

        assert result.exit_code == 0
        assert "probe.secure (query)" in result.stdout
        assert "Requires authentication: yes" in result.stdout
        assert "Idempotency key: not supported" in result.stdout

    def test_json(self) -> None:
        description = json.loads(_ops("describe", "notes.add", "--json", app_name="API").stdout)

        assert description["kind"] == "command"
        assert description["supports_idempotency_key"] is True
        assert description["input_schema"]["properties"]["text"]["type"] == "string"
        assert description["output_schema"]["properties"]["count"]["type"] == "integer"

    def test_an_unexposed_operation_is_usage(self) -> None:
        result = _ops("describe", "notes.add")

        assert result.exit_code == 2
        assert "No exposed operation 'notes.add'" in result.stderr


# ....................... #


class TestFindingTheDeclaration:
    def test_the_environment_names_it(self) -> None:
        result = runner.invoke(app, ["ops", "list"], env={"FORZE_OPS_APP": f"{_MODULE}:ANONYMOUS"})

        assert result.exit_code == 0
        assert "probe.echo" in result.stdout

    def test_a_callable_returning_one_is_accepted(self) -> None:
        assert _ops("list", app_name="lazy").exit_code == 0

    def test_none_named_is_usage(self) -> None:
        result = _ops("list", app_name=None)

        assert result.exit_code == 2
        assert "FORZE_OPS_APP" in result.stderr

    def test_a_malformed_reference_is_usage(self) -> None:
        result = runner.invoke(
            app, ["ops", "list", "--app", "no-colon"], env={"FORZE_OPS_APP": None}
        )

        assert result.exit_code == 2

    @pytest.mark.parametrize("ref", [f"{_MODULE}_missing:APP", f"{_MODULE}:unbuildable"])
    def test_a_failing_import_prints_the_traceback_and_exits_78(self, ref: str) -> None:
        result = runner.invoke(
            app, ["ops", "call", "probe.echo", "--app", ref], env={"FORZE_OPS_APP": None}
        )

        assert result.exit_code == 78
        assert result.stdout == ""
        assert "Traceback" in result.stderr

    def test_something_else_exits_78(self) -> None:
        result = _ops("list", app_name="NOT_AN_OPS_APP")

        assert result.exit_code == 78
        assert "is not an OpsApp" in result.stderr


# ....................... #


class TestEmbedding:
    def _cli(self) -> typer.Typer:
        root = typer.Typer(pretty_exceptions_show_locals=False)
        root.add_typer(build_ops_cli(sample.ANONYMOUS))

        @root.callback()
        def _root() -> None:  # pyright: ignore[reportUnusedFunction]
            """An application's own CLI."""

        return root

    def test_the_bound_group_needs_no_app(self) -> None:
        result = runner.invoke(
            self._cli(), ["ops", "call", "probe.echo", "--input", '{"text": "hi"}']
        )

        assert result.exit_code == 0
        assert json.loads(result.stdout) == {"text": "hi"}

    def test_the_bound_group_has_no_app_option(self) -> None:
        help_text = runner.invoke(self._cli(), ["ops", "list", "--help"]).stdout
        refused = runner.invoke(self._cli(), ["ops", "list", "--app", f"{_MODULE}:API"])

        assert "--app" not in help_text
        assert refused.exit_code == 2

    def test_the_docstring_warns_about_locals(self) -> None:
        assert "pretty_exceptions_show_locals=False" in (build_ops_cli.__doc__ or "")


def test_the_ops_help_renders_without_a_declaration() -> None:
    result = runner.invoke(app, ["ops", "--help"], env={"FORZE_OPS_APP": None})

    assert result.exit_code == 0
    assert "call" in result.stdout


def test_the_root_help_names_ops() -> None:
    result = runner.invoke(app, ["--help"])

    assert "ops" in result.stdout
    assert "OpsApp" in result.stdout
