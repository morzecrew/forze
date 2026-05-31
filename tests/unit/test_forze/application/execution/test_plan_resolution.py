"""Unit tests for :class:`PlanResolution` (no registry freeze)."""

from forze.application.contracts.execution import BeforeStep
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry.patch import PlanPatch
from forze.application.execution.operations.registry.resolution import PlanResolution
from forze.base.primitives import str_key_selector

# ----------------------- #


def _noop_before_factory(_ctx):
    async def _before(_args) -> None:
        return None

    return _before


def _tx_route_plan(route: str) -> OperationPlan:
    return OperationPlan().bind_tx().set_route(route).finish(deep=False)


def _outer_before_plan(step_id: str) -> OperationPlan:
    step = BeforeStep(id=step_id, factory=_noop_before_factory)

    return OperationPlan().bind_outer().before(step).finish(deep=False)


class TestPlanResolutionPatchOrder:
    def test_more_specific_patch_merges_after_broader_patch(self) -> None:
        resolution = PlanResolution(
            plans={},
            patches=(
                PlanPatch(
                    selector=str_key_selector.prefix("projects."),
                    plan=_tx_route_plan("pg"),
                ),
                PlanPatch(
                    selector=str_key_selector.exact("projects.create"),
                    plan=_outer_before_plan("narrow"),
                ),
            ),
        )

        create = resolution.resolve("projects.create").freeze()
        get = resolution.resolve("projects.get").freeze()
        other = resolution.resolve("other.create")

        assert create.tx.route == "pg"
        assert get.tx.route == "pg"
        assert other.tx_route() is None

        assert len(create.outer.before.steps) == 1
        assert len(get.outer.before.steps) == 0

    def test_all_keys_then_suffix_both_apply_to_matching_ops(self) -> None:
        resolution = PlanResolution(
            plans={},
            patches=(
                PlanPatch(
                    selector=str_key_selector.all_keys(),
                    plan=_tx_route_plan("base"),
                ),
                PlanPatch(
                    selector=str_key_selector.suffix(".create"),
                    plan=_outer_before_plan("create_only"),
                ),
            ),
        )

        create = resolution.resolve("projects.create").freeze()
        get = resolution.resolve("projects.get").freeze()

        assert create.tx.route == "base"
        assert get.tx.route == "base"
        assert len(create.outer.before.steps) == 1
        assert len(get.outer.before.steps) == 0

    def test_explicit_plan_merges_after_patches(self) -> None:
        explicit = _outer_before_plan("explicit")
        resolution = PlanResolution(
            plans={"op": explicit},
            patches=(
                PlanPatch(
                    selector=str_key_selector.all_keys(),
                    plan=_tx_route_plan("mock"),
                ),
            ),
        )

        resolved = resolution.resolve("op").freeze()

        assert resolved.tx.route == "mock"
        assert len(resolved.outer.before.steps) == 1
        assert "explicit" in resolved.outer.before.steps
