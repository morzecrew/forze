"""The mock-server load plan — operation keys, a sample and a declared write, run in-process.

The app is served through ``httpx.ASGITransport`` on the test's own event loop, so a short
two-step ramp needs no socket and no second process.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Skipped, not failed, without the extras the recipe and the kit need.
pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from examples.recipes.mock_server.app import build_server
from examples.recipes.mock_server.load import KEY_ENV, build_plan
from forze_load import ClosedRamp, run

# ----------------------- #

_RECIPE_KEY = "dev-key"  # the recipe's own identity.local.json; not a secret


async def test_the_plan_ramps_the_mock_server_by_operation_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(KEY_ENV, _RECIPE_KEY)
    app = build_server()
    out = tmp_path / "ramp.jsonl"

    # ASGITransport sends no lifespan events: open the runtime scope and the seed here.
    async with app.router.lifespan_context(app):
        result = await run(
            build_plan("http://localhost"),
            ClosedRamp(steps=[2, 4], step_s=0.6, warm_s=0.1),
            out=out,
            transport=httpx.ASGITransport(app=app),
        )

    assert result.meta["openapi_sha256"]
    assert [row["workers"] for row in result.rows] == [2, 4]

    for row in result.rows:
        assert row["outcomes"]["ok"] > 0
        assert row["outcomes"]["forbidden"] == 0
        assert row["outcomes"]["not_found"] == 0
        assert row["outcomes"]["server_error"] == 0
        assert not row["misconfigured"]
        # In-process, the app shares the loop's thread, so its CPU is not judged.
        assert row["client"]["loop_cpu_judged"] is False

    scenarios = set().union(*(row["scenarios"] for row in result.rows))
    assert {"products.get", "list", "create"} <= scenarios
    assert result.summary.get("stopped") is None
    assert _RECIPE_KEY not in out.read_text()
