"""Freeze-time hedging safety gate (validate_hedge_safety) + markers."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import (
    DeclaresHedge,
    MiddlewareStep,
    ProvidesIdempotency,
)
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.contracts.resilience import HedgeSafety
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry.patch import PlanPatch
from forze.application.execution.operations.registry.resolution import PlanResolution
from forze.application.execution.operations.registry.validation import (
    RegistryFreezeValidator,
)
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.application.hooks.resilience import HedgeWrap, ResilienceWrap
from forze.base.exceptions import CoreException
from forze.base.primitives import str_key_selector

# ----------------------- #


class _Result(BaseModel):
    pass


def _idempotency_step() -> MiddlewareStep:
    return IdempotencyWrap(
        op="op", spec=IdempotencySpec(name="s"), result_type=_Result
    ).to_step()


def _handlers() -> dict[str, Any]:
    def factory(_ctx: Any) -> Any:
        async def handler(_args: Any) -> None:  # pragma: no cover - never invoked
            return None

        return handler

    return {"op": factory}


def _validate(*steps: MiddlewareStep) -> None:
    plan = OperationPlan().bind_outer().wrap(*steps).finish(deep=False)
    resolution = PlanResolution(
        plans={},
        patches=(PlanPatch(selector=str_key_selector.exact("op"), plan=plan),),
    )
    RegistryFreezeValidator.validate_hedge_safety(_handlers(), resolution)


class TestHedgeSafetyGate:
    def test_hedge_without_guard_or_safety_raises(self) -> None:
        with pytest.raises(CoreException, match="hedged"):
            _validate(HedgeWrap(policy="p").to_step())

    def test_hedge_with_explicit_safety_ok(self) -> None:
        _validate(HedgeWrap(policy="p", safety=HedgeSafety.READ_ONLY).to_step())

    def test_hedge_with_idempotency_sibling_still_requires_explicit_safety(self) -> None:
        # A boundary IdempotencyWrap is claimed once *outside* the hedge, so it cannot make
        # the hedge's concurrent duplicate attempts safe: the gate must still reject it.
        with pytest.raises(CoreException, match="hedged"):
            _validate(HedgeWrap(policy="p").to_step(), _idempotency_step())

    def test_hedge_with_idempotency_and_explicit_safety_ok(self) -> None:
        _validate(
            HedgeWrap(policy="p", safety=HedgeSafety.IDEMPOTENT).to_step(),
            _idempotency_step(),
        )

    def test_no_hedge_is_unaffected(self) -> None:
        _validate(ResilienceWrap(policy="p").to_step())


class TestMarkers:
    def test_hedge_wrap_declares_hedge(self) -> None:
        assert isinstance(HedgeWrap(policy="p"), DeclaresHedge)
        assert HedgeWrap(policy="p").hedge_safety_declared() is False
        assert (
            HedgeWrap(policy="p", safety=HedgeSafety.IDEMPOTENT).hedge_safety_declared()
            is True
        )

    def test_idempotency_wrap_provides_idempotency(self) -> None:
        wrap = IdempotencyWrap(
            op="op", spec=IdempotencySpec(name="s"), result_type=_Result
        )
        assert isinstance(wrap, ProvidesIdempotency)
        assert wrap.provides_idempotency() is True

    def test_resilience_wrap_is_neither(self) -> None:
        wrap = ResilienceWrap(policy="p")
        assert not isinstance(wrap, DeclaresHedge)
        assert not isinstance(wrap, ProvidesIdempotency)
