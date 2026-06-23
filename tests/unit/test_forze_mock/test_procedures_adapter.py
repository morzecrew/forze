"""Tests for MockProceduresAdapter and ctx.procedures resolution."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.procedures import ExecResult, ProcedureSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockProcedureRegistry, MockState
from forze_mock.adapters.procedures import MockProceduresAdapter
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Params(BaseModel):
    window: str = "2026-01-01"


class _RowOut(BaseModel):
    total: int = 0


def _spec(result: type | None = None) -> ProcedureSpec[_Params, object]:
    return ProcedureSpec(name="recompute", params=_Params, result=result)


# ----------------------- #
# ctx wiring


@pytest.mark.asyncio
async def test_command_via_ctx_runs_handler() -> None:
    registry = MockProcedureRegistry().on(
        "recompute",
        lambda params, state: ExecResult(affected_count=3),
    )
    ctx: ExecutionContext = context_from_deps(MockDepsModule(procedures=registry)())

    result = await ctx.procedures.command(_spec()).run(_Params())

    assert result.affected_count == 3


@pytest.mark.asyncio
async def test_handler_computes_over_mock_state() -> None:
    # A recompute that reads seeded in-memory state and returns a derived count.
    state = MockState()
    state.analytics_ingest_log["batch"] = [{"x": 1}, {"x": 2}, {"x": 3}]

    def _recompute(params: BaseModel, st: MockState) -> ExecResult[int]:
        rows = st.analytics_ingest_log.get("batch", [])
        return ExecResult(value=len(rows))

    registry = MockProcedureRegistry().on("recompute", _recompute)
    ctx = context_from_deps(MockDepsModule(state=state, procedures=registry)())

    result = await ctx.procedures.command(_spec(result=int)).run(_Params())

    assert result.value == 3


@pytest.mark.asyncio
async def test_async_handler_is_awaited() -> None:
    async def _handler(params: BaseModel, state: MockState) -> ExecResult[_RowOut]:
        return ExecResult(value=_RowOut(total=7))

    registry = MockProcedureRegistry().on("recompute", _handler)
    ctx = context_from_deps(MockDepsModule(procedures=registry)())

    result = await ctx.procedures.command(_spec(result=_RowOut)).run(_Params())

    assert isinstance(result.value, _RowOut)
    assert result.value.total == 7


@pytest.mark.asyncio
async def test_unprogrammed_procedure_raises() -> None:
    ctx = context_from_deps(MockDepsModule()())  # no registry

    with pytest.raises(CoreException, match="mock.procedures.unprogrammed"):
        await ctx.procedures.command(_spec()).run(_Params())


@pytest.mark.asyncio
async def test_params_must_be_spec_type() -> None:
    class _Other(BaseModel):
        x: int = 1

    registry = MockProcedureRegistry().on(
        "recompute", lambda p, s: ExecResult(affected_count=1)
    )
    ctx = context_from_deps(MockDepsModule(procedures=registry)())

    with pytest.raises(CoreException, match="must be a _Params instance"):
        await ctx.procedures.command(_spec()).run(_Other())  # type: ignore[arg-type]


# ----------------------- #
# tenancy parity (direct adapter)


@pytest.mark.asyncio
async def test_tenant_aware_fails_closed_without_tenant() -> None:
    registry = MockProcedureRegistry().on(
        "recompute", lambda p, s: ExecResult(affected_count=1)
    )
    adapter = MockProceduresAdapter(
        state=MockState(),
        spec=_spec(),
        registry=registry,
        tenant_aware=True,
        tenant_provider=lambda: None,
    )

    with pytest.raises(CoreException, match="tenant_required"):
        await adapter.run(_Params())


@pytest.mark.asyncio
async def test_tenant_aware_runs_with_bound_tenant() -> None:
    from uuid import uuid4

    registry = MockProcedureRegistry().on(
        "recompute", lambda p, s: ExecResult(affected_count=5)
    )
    adapter = MockProceduresAdapter(
        state=MockState(),
        spec=_spec(),
        registry=registry,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=uuid4()),
    )

    result = await adapter.run(_Params())
    assert result.affected_count == 5
