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

    def test_the_command_is_registered_with_its_own_help(self) -> None:
        result = runner.invoke(app, ["mock", "--help"])

        assert result.exit_code == 0
        assert "serve" in result.output
