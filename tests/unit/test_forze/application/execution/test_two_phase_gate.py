"""Freeze-time gate: a two-phase (prepare/apply) operation requires a tx route."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry.patch import PlanPatch
from forze.application.execution.operations.registry.resolution import PlanResolution
from forze.application.execution.operations.registry.validation import (
    RegistryFreezeValidator,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import str_key_selector

# ----------------------- #


def _handlers() -> dict[str, Any]:
    def factory(_ctx: Any) -> Any:  # pragma: no cover - never invoked
        async def handler(_args: Any) -> None:
            return None

        return handler

    return {"op": factory}


def _plan(*, two_phase: bool = True, route: str | None = "pg") -> OperationPlan:
    plan = OperationPlan(two_phase=two_phase)
    tx = plan.bind_tx()

    if route is not None:
        tx = tx.set_route(route)

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
        _validate(_plan())

    def test_non_two_phase_without_route_is_unaffected(self) -> None:
        _validate(_plan(two_phase=False, route=None))
