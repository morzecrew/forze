"""The same operation through ``forze ops call`` and through MCP dispatch agrees.

Both surfaces run the shared ingress, so a result, a denial code and a masked validation
error must come out the same; only each surface's own error codes differ.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

pytest.importorskip("fastmcp")

from typer.testing import CliRunner

from forze.base.exceptions import CoreException
from forze_cli.app import app
from forze_mcp.dispatch import invoke_operation
from forze_mcp.identity import StaticIdentityResolver
from tests.support import ops_cli_app as sample

# ----------------------- #

runner = CliRunner()


@pytest.fixture(autouse=True)
def _seeded() -> None:
    sample.seed()


def _cli(op: str, arguments: dict[str, Any]) -> Any:
    options = ["--input", json.dumps(arguments)] if arguments else []
    result = runner.invoke(
        app,
        ["ops", "call", op, *options, "--app", "tests.support.ops_cli_app:ANONYMOUS"],
        env={"FORZE_OPS_APP": None},
    )

    return json.loads(result.stdout)


def _mcp(op: str, arguments: dict[str, Any]) -> Any:
    async def run() -> Any:
        runtime = sample.runtime()

        async with runtime.scope():
            return await invoke_operation(
                registry=sample.REGISTRY,
                ctx_factory=runtime.get_context,
                identity=StaticIdentityResolver(),
                op=op,
                descriptor=sample.REGISTRY.catalog()[op].descriptor,
                arguments=arguments,
            )

    return asyncio.run(run())


# ....................... #


def test_an_equal_result() -> None:
    assert _cli("probe.echo", {"text": "hi"}) == _mcp("probe.echo", {"text": "hi"}).model_dump(
        mode="json"
    )


def test_an_equal_denial_code() -> None:
    with pytest.raises(CoreException) as caught:
        _mcp("probe.secure", {})

    assert _cli("probe.secure", {})["error"]["code"] == caught.value.code == "auth_required"


def test_an_equally_masked_validation_error() -> None:
    wrong = {"text": {"password": "s3cr3t-value"}}

    with pytest.raises(CoreException) as caught:
        _mcp("probe.echo", wrong)

    cli = _cli("probe.echo", wrong)["error"]

    assert caught.value.details is not None
    # Compared as JSON: the document is JSON, the exception holds tuples.
    assert cli["context"]["errors"] == json.loads(json.dumps(caught.value.details["errors"]))
    assert "s3cr3t-value" not in json.dumps(cli)
