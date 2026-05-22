"""Unit tests for operation plan scope binding."""

import pytest

from forze.application.contracts.execution import BeforeStep
from forze.application.execution.planning import OperationPlan
from forze.application.execution.registry import OperationRegistry
from forze.base.errors import CoreError


def _noop_before_factory(_ctx):
    async def _before(_args) -> None:
        return None

    return _before


class TestRegistryPlanBinding:
    def test_bind_outer_accumulates_before_steps(self) -> None:
        step = BeforeStep(id="b1", factory=_noop_before_factory)
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: None})
            .bind("op")
            .bind_outer()
            .before(step)
            .finish(deep=True)
        )
        plans = reg.get_plans()
        assert len(plans["op"]._outer.before.items) == 1

    def test_finish_without_ops_raises(self) -> None:
        with pytest.raises(CoreError, match="No operations"):
            OperationRegistry().bind()
