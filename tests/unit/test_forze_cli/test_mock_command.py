"""``forze mock serve`` — resolution and refusals.

Serving itself binds a port, so what is worth testing here is everything up to that: the
command resolves the same ``module:attr`` contract ``forze dst`` does, refuses anything that
is not a ``MockApp``, and passes the environment gate through rather than working around it.
"""

from __future__ import annotations

import pytest

pytest.importorskip("typer")
pytest.importorskip("starlette")

from typer.testing import CliRunner

from forze_cli.app import app

pytestmark = pytest.mark.unit

_RECIPE = "examples.recipes.mock_server.served:mock_app"

runner = CliRunner()


class TestMockServe:
    def test_it_refuses_without_the_environment_gate(self, monkeypatch) -> None:
        monkeypatch.delenv("FORZE_MOCK_SERVER", raising=False)

        result = runner.invoke(app, ["mock", "serve", _RECIPE])

        assert result.exit_code != 0
        assert "FORZE_MOCK_SERVER=1" in str(result.exception)

    def test_it_refuses_an_object_that_is_not_a_mock_app(self, monkeypatch) -> None:
        monkeypatch.setenv("FORZE_MOCK_SERVER", "1")

        result = runner.invoke(
            app, ["mock", "serve", "examples.recipes.mock_server.app:product_spec"]
        )

        assert result.exit_code == 1
        assert "is not a MockApp" in result.output

    def test_a_callable_that_cannot_be_called_bare_is_guidance_not_a_traceback(
        self, monkeypatch
    ) -> None:
        # `MockApp` itself, and any factory taking arguments, are both callable — so both
        # reach `target()` and both are the same user mistake as naming the wrong attribute.
        monkeypatch.setenv("FORZE_MOCK_SERVER", "1")

        result = runner.invoke(app, ["mock", "serve", "forze_mock.server:MockApp"])

        assert result.exit_code == 1
        assert result.exception is None or isinstance(result.exception, SystemExit)
        assert "zero-argument callable" in result.output

    def test_a_factory_that_fails_on_the_inside_keeps_its_traceback(self, monkeypatch) -> None:
        # The signature is checked before the call precisely so this stays a real error: a
        # factory raising TypeError internally is a bug, not "expose a MockApp".
        import forze_cli.mock as mock_command

        def _broken_factory() -> object:
            raise TypeError("the factory itself is broken")

        monkeypatch.setenv("FORZE_MOCK_SERVER", "1")
        monkeypatch.setattr(mock_command, "load_object", lambda _ref: _broken_factory)

        result = runner.invoke(app, ["mock", "serve", "anything:at_all"])

        assert isinstance(result.exception, TypeError)
        assert "the factory itself is broken" in str(result.exception)
        assert "zero-argument callable" not in result.output

    def test_a_target_with_no_introspectable_signature_is_still_called(self, monkeypatch) -> None:
        # Some builtins have no signature to bind against. The check cannot conclude anything
        # there, so it must stand aside rather than refuse a target that would have worked.
        import forze_cli.mock as mock_command

        monkeypatch.setenv("FORZE_MOCK_SERVER", "1")
        # `range` is a builtin with no introspectable signature, and calling it bare raises —
        # so a guard that did not stand aside would answer with its own message instead.
        monkeypatch.setattr(mock_command, "load_object", lambda _ref: range)

        result = runner.invoke(app, ["mock", "serve", "anything:at_all"])

        assert "cannot be called without arguments" not in result.output
        assert isinstance(result.exception, TypeError), result.output

    def test_serving_does_not_require_the_seed_generator(self, monkeypatch) -> None:
        # A MockApp with no seed plan never imports `forze_mock.seeding`, so gating every
        # served app on the generator refuses apps over a dependency they do not use.
        import forze_cli._compat as compat

        seen: list[str] = []
        monkeypatch.setattr(
            compat, "find_spec", lambda name: seen.append(name) or object()  # type: ignore[func-returns-value]
        )

        compat.require_mock_server()

        assert "polyfactory" not in seen
        assert seen == ["starlette", "uvicorn"]

    def test_the_command_is_registered_with_its_own_help(self) -> None:
        result = runner.invoke(app, ["mock", "--help"])

        assert result.exit_code == 0
        assert "serve" in result.output
