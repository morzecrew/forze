"""The active-operation guard: constructing an ExecutionContext mid-operation warns.

ExecutionContext owns per-instance ContextVars and per-scope caches, so it must
be created once per runtime scope. Creating one while an operation is executing
is the signature of per-request creation (an unsupported mode) and must trip
the module-level tripwire.
"""

from __future__ import annotations

import attrs
import pytest
import structlog.testing

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.context import active_operation
from forze.application.execution.context.active_operation import (
    is_operation_running,
    operation_running,
)
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


def _frozen(op: str, factory):
    return OperationRegistry(handlers={op: factory}).bind(op).finish().freeze()


@attrs.define(slots=True)
class _ProbeMarker(Handler[None, bool]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> bool:
        return is_operation_running()


@attrs.define(slots=True)
class _FailWithMarker(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        assert is_operation_running() is True
        raise RuntimeError("boom")


@attrs.define(slots=True)
class _ConstructContext(Handler[None, str]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> str:
        _ctx()  # the misuse: a fresh ExecutionContext inside a request
        return "done"


# ....................... #


class TestActiveOperationMarker:
    async def test_marker_set_during_operation_and_reset_after(self) -> None:
        ctx = _ctx()
        reg = _frozen("probe", _ProbeMarker)

        assert is_operation_running() is False
        assert await run_operation(reg, "probe", None, ctx) is True
        assert is_operation_running() is False

    async def test_marker_reset_when_handler_raises(self) -> None:
        # The engine sets/resets the marker with a raw token pair (not the
        # operation_running CM): the reset must still happen on a handler error.
        ctx = _ctx()
        reg = _frozen("fail", _FailWithMarker)

        with pytest.raises(RuntimeError, match="boom"):
            await run_operation(reg, "fail", None, ctx)

        assert is_operation_running() is False

    async def test_operation_running_cm_is_token_reset(self) -> None:
        assert is_operation_running() is False

        with operation_running():
            assert is_operation_running() is True

            with operation_running():
                assert is_operation_running() is True

            assert is_operation_running() is True

        assert is_operation_running() is False


class TestContextConstructionGuard:
    async def test_construction_inside_operation_warns_once(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(active_operation, "_warned_ctx_in_operation", False)

        ctx = _ctx()
        reg = _frozen("misuse", _ConstructContext)

        with structlog.testing.capture_logs() as logs:
            await run_operation(reg, "misuse", None, ctx)

        warnings = [
            e
            for e in logs
            if e["log_level"] == "warning"
            and "inside an active operation" in e["event"]
        ]
        assert len(warnings) == 1

        # A second occurrence logs at debug, not warning.
        with structlog.testing.capture_logs() as logs:
            await run_operation(reg, "misuse", None, ctx)

        assert not [
            e
            for e in logs
            if e["log_level"] == "warning"
            and "inside an active operation" in e["event"]
        ]

    async def test_construction_outside_operation_is_silent(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(active_operation, "_warned_ctx_in_operation", False)

        with structlog.testing.capture_logs() as logs:
            _ctx()

        assert not [
            e for e in logs if "inside an active operation" in e.get("event", "")
        ]
        assert active_operation._warned_ctx_in_operation is False

    async def test_construction_after_operation_is_silent(
        self,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(active_operation, "_warned_ctx_in_operation", False)

        ctx = _ctx()
        reg = _frozen("probe", _ProbeMarker)
        await run_operation(reg, "probe", None, ctx)

        with structlog.testing.capture_logs() as logs:
            _ctx()

        assert not [
            e for e in logs if "inside an active operation" in e.get("event", "")
        ]
