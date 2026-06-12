"""Plain merge of operation registry handlers, plans, and patches."""

from typing import Self, final

import attrs

from forze.application.contracts.execution import HandlerFactory
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from ..descriptors import OperationDescriptor
from ..planning import OperationPlan
from .patch import PlanPatch

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RegistryMerge:
    """Merged handlers, plans, and patches without resolution or validation."""

    handlers: StrKeyMapping[HandlerFactory] = attrs.field(
        factory=dict[StrKey, HandlerFactory],
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Handler factories for operations."""

    plans: StrKeyMapping[OperationPlan] = attrs.field(
        factory=dict[StrKey, OperationPlan],
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Execution plans for operations."""

    descriptors: StrKeyMapping[OperationDescriptor] = attrs.field(
        factory=dict[StrKey, OperationDescriptor],
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Catalog metadata for operations."""

    patches: tuple[PlanPatch, ...] = attrs.field(factory=tuple)
    """Plan patches applied by selector at freeze."""

    # ....................... #

    @hybridmethod
    def merge(cls: type[Self], *parts: Self, override: bool = False) -> Self:  # type: ignore[misc, override]
        """Merge multiple registry contents into one.

        Duplicate operation keys (handlers, plans, descriptors) and duplicate patch
        selectors are a configuration error naming the colliding keys — merging is
        meant to combine *disjoint* registries. Pass ``override=True`` to explicitly
        let later parts replace earlier ones instead.
        """

        merged_handlers: dict[StrKey, HandlerFactory] = {}
        merged_plans: dict[StrKey, OperationPlan] = {}
        merged_descriptors: dict[StrKey, OperationDescriptor] = {}
        merged_patches: list[PlanPatch] = []

        def _conflict(kind: str, keys: set[str]) -> CoreException:
            return exc.configuration(
                f"Operation registry merge conflict — duplicate {kind} for "
                f"operations: {sorted(keys)}. Later registries would silently "
                "replace earlier ones; pass override=True to allow that explicitly."
            )

        for part in parts:
            if not override:
                handler_conflicts = set(map(str, merged_handlers.keys())) & set(
                    map(str, part.handlers.keys())
                )
                plan_conflicts = set(map(str, merged_plans.keys())) & set(
                    map(str, part.plans.keys())
                )
                descriptor_conflicts = set(map(str, merged_descriptors.keys())) & set(
                    map(str, part.descriptors.keys())
                )

                if handler_conflicts:
                    raise _conflict("handler factories", handler_conflicts)

                if plan_conflicts:
                    raise _conflict("operation plans", plan_conflicts)

                if descriptor_conflicts:
                    raise _conflict("operation descriptors", descriptor_conflicts)

            for patch in part.patches:
                colliding = next(
                    (
                        index
                        for index, existing in enumerate(merged_patches)
                        if existing.selector == patch.selector
                    ),
                    None,
                )

                if colliding is None:
                    merged_patches.append(patch)

                elif override:
                    merged_patches[colliding] = patch

                else:
                    raise exc.configuration(
                        "Operation registry merge conflict — duplicate plan patch "
                        f"selector: {patch.selector!r}. Pass override=True to let "
                        "the later patch replace the earlier one."
                    )

            merged_handlers.update(part.handlers)
            merged_plans.update(part.plans)
            merged_descriptors.update(part.descriptors)

        return cls(
            handlers=merged_handlers,
            plans=merged_plans,
            descriptors=merged_descriptors,
            patches=tuple(merged_patches),
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: Self, *parts: Self, override: bool = False) -> Self:  # type: ignore[misc, override]
        return type(self).merge(self, *parts, override=override)
