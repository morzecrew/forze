"""Plain merge of operation registry handlers, plans, and patches."""

from typing import Self, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import HandlerFactory
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import (
    MappingConverter,
    StrKey,
    StrKeyMapping,
    StrKeySelector,
    str_key_selector,
)

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
    def merge(  # type: ignore[misc, override]
        cls: type[Self],  # type: ignore[misc, override]
        *parts: Self,
        override: bool = False,
        cross_registry: bool = False,
    ) -> Self:
        """
        Merge multiple registry contents into one.
        
        Duplicate operation keys (handlers, plans, descriptors) and duplicate patch
        selectors raise a configuration error unless override=True. Plan patches are
        late-bound and resolve against the full merged key set, so a patch from one
        part may reach operations contributed by another. Cross-registry patch reach
        raises a configuration error unless cross_registry=True; when allowed, it is
        logged. Patches that reach only operations from their own part are not flagged.
        """

        merged_handlers: dict[StrKey, HandlerFactory] = {}
        merged_plans: dict[StrKey, OperationPlan] = {}
        merged_descriptors: dict[StrKey, OperationDescriptor] = {}
        merged_patches: list[PlanPatch] = []

        # Selector -> foreign operation keys it would govern across the boundary.
        crossings: dict[StrKeySelector.Spec, set[str]] = {}

        def _conflict(kind: str, keys: set[str]) -> CoreException:
            """
            Construct a configuration exception for duplicate registry entries.
            
            Parameters:
                kind (str): Category of duplicate entries (e.g., 'handler factories', 'operation plans').
                keys (set[str]): Operation keys that are duplicated.
            
            Returns:
                CoreException: A configuration exception with details about the conflict.
            """
            return exc.configuration(
                f"Operation registry merge conflict — duplicate {kind} for "
                f"operations: {sorted(keys)}. Later registries would silently "
                "replace earlier ones; pass override=True to allow that explicitly."
            )

        def _record_crossings(
            patches: "tuple[PlanPatch, ...] | list[PlanPatch]",
            handlers: StrKeyMapping[HandlerFactory],
        ) -> None:
            """
            Record which operations each plan patch selector would govern.
            
            Parameters:
            	patches (tuple[PlanPatch, ...] | list[PlanPatch]): Collection of plan patches to check
            	handlers (StrKeyMapping[HandlerFactory]): Mapping of operations to check against patch selectors
            """
            for patch in patches:
                for op in handlers:
                    if str_key_selector.matches(patch.selector, str(op)):
                        crossings.setdefault(patch.selector, set()).add(str(op))

        for part in parts:
            # Detect cross-registry patch reach before this part's handlers/patches
            # join the accumulator, so the comparison is strictly against *other*
            # parts: this part's patches over already-merged ops, and already-merged
            # patches over this part's ops.
            _record_crossings(part.patches, merged_handlers)
            _record_crossings(merged_patches, part.handlers)

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

        if crossings:
            detail = "; ".join(
                f"{selector!r} → {sorted(ops)}"
                for selector, ops in sorted(
                    crossings.items(), key=lambda item: repr(item[0])
                )
            )

            if not cross_registry:
                raise exc.configuration(
                    "Operation registry merge — plan patch(es) reach operations "
                    f"contributed by another registry: {detail}. A late-bound "
                    "patch governs another registry's operations only because they "
                    "share the merged key set. Scope it with namespace=, settle it "
                    "with materialize_patches() before merging, or pass "
                    "cross_registry=True to allow this explicitly."
                )

            logger.info(
                "Operation registry merge — plan patch(es) reach operations from "
                "another registry (allowed via cross_registry=True): %s",
                detail,
            )

        return cls(
            handlers=merged_handlers,
            plans=merged_plans,
            descriptors=merged_descriptors,
            patches=tuple(merged_patches),
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # type: ignore[misc, override]
        self: Self,
        *parts: Self,
        override: bool = False,
        cross_registry: bool = False,
    ) -> Self:
        """
        Merge this registry with others, with configurable handling of duplicates and cross-registry patches.
        
        Parameters:
            *parts (RegistryMerge): Additional registries to merge with this one.
            override (bool): If True, allows duplicate keys in handlers, plans, or descriptors, with earlier entries replaced. Defaults to False.
            cross_registry (bool): If True, allows plan patches to reach operations from other registries. If False, raises an error on cross-registry patch reach. Defaults to False.
        
        Returns:
            RegistryMerge: A new merged registry combining handlers, plans, descriptors, and patches from all parts.
        """
        return type(self).merge(
            self, *parts, override=override, cross_registry=cross_registry
        )
