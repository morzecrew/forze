"""Freeze-time two-phase (prepare/apply) gates + the MayReplayHandler marker."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.execution import (
    MayReplayHandler,
    MiddlewareStep,
)
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry.patch import PlanPatch
from forze.application.execution.operations.registry.resolution import PlanResolution
from forze.application.execution.operations.registry.validation import (
    RegistryFreezeValidator,
)
from forze.application.hooks.resilience import HedgeWrap, ResilienceWrap
from forze.base.exceptions import CoreException
from forze.base.primitives import str_key_selector

# ----------------------- #


def _handlers() -> dict[str, Any]:
    def factory(_ctx: Any) -> Any:  # pragma: no cover - never invoked
        async def handler(_args: Any) -> None:
            return None

        return handler

    return {"op": factory}


def _plan(
    *,
    two_phase: bool = True,
    rerun_safe: bool = False,
    route: str | None = "pg",
    outer_wraps: tuple[MiddlewareStep, ...] = (),
    tx_wraps: tuple[MiddlewareStep, ...] = (),
) -> OperationPlan:
    plan = OperationPlan(two_phase=two_phase, prepare_rerun_safe=rerun_safe)

    if outer_wraps:
        plan = plan.bind_outer().wrap(*outer_wraps).finish(deep=False)

    tx = plan.bind_tx()

    if route is not None:
        tx = tx.set_route(route)

    if tx_wraps:
        tx = tx.wrap(*tx_wraps)

    return tx.finish(deep=False)


def _validate(plan: OperationPlan) -> None:
    resolution = PlanResolution(
        plans={},
        patches=(PlanPatch(selector=str_key_selector.exact("op"), plan=plan),),
    )
    RegistryFreezeValidator.validate_two_phase(_handlers(), resolution)


# ....................... #


class TestTwoPhaseRouteGate:
    def test_two_phase_without_route_raises(self) -> None:
        with pytest.raises(CoreException, match="two-phase"):
            _validate(_plan(route=None))

    def test_two_phase_with_route_ok(self) -> None:
        _validate(_plan())  # route set, no replay wrap

    def test_non_two_phase_without_route_is_unaffected(self) -> None:
        _validate(_plan(two_phase=False, route=None))


class TestPrepareRerunSafetyGate:
    def test_resilience_wrap_without_rerun_safe_raises(self) -> None:
        with pytest.raises(CoreException, match="re-run"):
            _validate(_plan(outer_wraps=(ResilienceWrap(policy="p").to_step(),)))

    def test_resilience_wrap_with_rerun_safe_ok(self) -> None:
        _validate(
            _plan(
                rerun_safe=True,
                outer_wraps=(ResilienceWrap(policy="p").to_step(),),
            )
        )

    def test_hedge_wrap_without_rerun_safe_raises(self) -> None:
        with pytest.raises(CoreException, match="re-run"):
            _validate(_plan(outer_wraps=(HedgeWrap(policy="p").to_step(),)))

    def test_tx_scope_replay_wrap_does_not_trigger(self) -> None:
        # A replay wrap on the TRANSACTION scope runs around apply only (inside the
        # tx), so it never re-runs prepare — the gate must ignore it.
        _validate(_plan(tx_wraps=(ResilienceWrap(policy="p").to_step(),)))


class TestMayReplayMarker:
    def test_resilience_and_hedge_declare_may_replay(self) -> None:
        assert isinstance(ResilienceWrap(policy="p"), MayReplayHandler)
        assert ResilienceWrap(policy="p").may_replay_handler() is True
        assert isinstance(HedgeWrap(policy="p"), MayReplayHandler)
        assert HedgeWrap(policy="p").may_replay_handler() is True
