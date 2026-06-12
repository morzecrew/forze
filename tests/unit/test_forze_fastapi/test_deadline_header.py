"""Inbound deadline-budget header binding (opt-in) on the invocation middleware."""

from __future__ import annotations

from fastapi import FastAPI
from starlette.testclient import TestClient

from forze.application.contracts.envelope import HTTP_HEADER_DEADLINE_BUDGET
from forze.application.execution import ExecutionContext
from forze.application.execution.context import remaining_time
from forze_fastapi.middlewares import InvocationMetadataMiddleware
from forze_fastapi.middlewares.invocation import _parse_budget_header
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #


def _app(*, bind_deadline: bool) -> tuple[FastAPI, dict[str, float | None]]:
    ctx: ExecutionContext = context_from_deps(MockDepsModule(state=MockState())())
    seen: dict[str, float | None] = {}

    app = FastAPI()
    app.add_middleware(
        InvocationMetadataMiddleware,
        ctx_dep=lambda: ctx,
        bind_deadline_from_header=bind_deadline,
    )

    @app.get("/probe")
    async def probe() -> dict[str, bool]:
        seen["remaining"] = remaining_time()
        return {"ok": True}

    return app, seen


class TestDeadlineHeader:
    def test_opt_in_binds_budget(self) -> None:
        app, seen = _app(bind_deadline=True)

        with TestClient(app) as client:
            client.get("/probe", headers={HTTP_HEADER_DEADLINE_BUDGET: "5.0"})

        assert seen["remaining"] is not None
        assert 0.0 < seen["remaining"] <= 5.0

    def test_default_off_ignores_header(self) -> None:
        app, seen = _app(bind_deadline=False)

        with TestClient(app) as client:
            client.get("/probe", headers={HTTP_HEADER_DEADLINE_BUDGET: "5.0"})

        assert seen["remaining"] is None

    def test_absent_header_binds_nothing(self) -> None:
        app, seen = _app(bind_deadline=True)

        with TestClient(app) as client:
            client.get("/probe")

        assert seen["remaining"] is None

    def test_malformed_values_ignored(self) -> None:
        assert _parse_budget_header(None) is None
        assert _parse_budget_header("nope") is None
        assert _parse_budget_header("-1") is None
        assert _parse_budget_header("0") is None
        assert _parse_budget_header("inf") is None
        assert _parse_budget_header("nan") is None
        assert _parse_budget_header("2.5") == 2.5
