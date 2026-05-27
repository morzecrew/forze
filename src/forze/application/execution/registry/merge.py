"""Plain merge of operation registry handlers, plans, and patches."""

from __future__ import annotations

from typing import Mapping, Self, final

import attrs

from forze.application.contracts.execution import HandlerFactory
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..planning import OperationPlan
from .patch import PlanPatch

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RegistryMerge:
    """Merged handlers, plans, and patches without resolution or validation."""

    handlers: Mapping[StrKey, HandlerFactory] = attrs.field(
        factory=dict[StrKey, HandlerFactory],
    )
    """Handler factories for operations."""

    plans: Mapping[StrKey, OperationPlan] = attrs.field(
        factory=dict[StrKey, OperationPlan],
    )
    """Execution plans for operations."""

    patches: tuple[PlanPatch, ...] = attrs.field(factory=tuple)
    """Plan patches applied by selector at freeze."""

    # ....................... #

    @hybridmethod
    def merge(cls: type[Self], *parts: Self) -> Self:  # type: ignore[misc, override]
        """Merge multiple registry contents into one."""

        merged_handlers: dict[StrKey, HandlerFactory] = {}
        merged_plans: dict[StrKey, OperationPlan] = {}
        merged_patches: list[PlanPatch] = []

        for part in parts:
            handler_conflicts = set(map(str, merged_handlers.keys())) & set(
                map(str, part.handlers.keys())
            )
            plan_conflicts = set(map(str, merged_plans.keys())) & set(
                map(str, part.plans.keys())
            )

            if handler_conflicts:
                raise exc.internal(
                    f"Conflicting handler factories: {handler_conflicts}"
                )

            if plan_conflicts:
                raise exc.internal(f"Conflicting operation plans: {plan_conflicts}")

            for patch in part.patches:
                if any(
                    existing.selector == patch.selector for existing in merged_patches
                ):
                    raise exc.internal(
                        f"Conflicting operation plan patches: {patch.selector!r}"
                    )

                merged_patches.append(patch)

            merged_handlers.update(part.handlers)
            merged_plans.update(part.plans)

        return cls(
            handlers=merged_handlers,
            plans=merged_plans,
            patches=tuple(merged_patches),
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: Self, *parts: Self) -> Self:  # type: ignore[misc, override]
        return type(self).merge(self, *parts)
