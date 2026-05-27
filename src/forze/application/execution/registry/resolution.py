"""Plan resolution for operation keys (patches + explicit plans)."""

from __future__ import annotations

from typing import Mapping, final

import attrs

from forze.base.primitives import StrKey, str_key_selector

from ..planning import OperationPlan
from .patch import PlanPatch

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PlanResolution:
    """Resolve effective operation plans from patches and per-op plans."""

    plans: Mapping[StrKey, OperationPlan] = attrs.field(
        factory=dict[StrKey, OperationPlan],
    )
    """Explicit per-operation plans."""

    patches: tuple[PlanPatch, ...] = attrs.field(factory=tuple)
    """Plan patches applied by selector."""

    # ....................... #

    def patch_indices_by_specificity(self) -> tuple[int, ...]:
        """Patch indices ordered by ascending specificity, then registration order."""

        indices = tuple(range(len(self.patches)))

        return tuple(
            sorted(
                indices,
                key=lambda i: (
                    str_key_selector.specificity(self.patches[i].selector),
                    i,
                ),
            ),
        )

    # ....................... #

    def resolve(self, op: str) -> OperationPlan:
        """Resolve the effective plan for an operation key."""

        plan = OperationPlan()

        for index in self.patch_indices_by_specificity():
            patch = self.patches[index]

            if str_key_selector.matches(patch.selector, op):
                plan = plan.merge(patch.plan)

        explicit = self.plans.get(op)

        if explicit is not None:
            plan = plan.merge(explicit)

        return plan
