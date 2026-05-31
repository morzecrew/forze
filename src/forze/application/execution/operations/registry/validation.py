"""Freeze-time validation for operation registries."""

from __future__ import annotations

from collections import defaultdict
from typing import Mapping, final

from forze.application.contracts.execution import HandlerFactory
from forze.base.exceptions import exc
from forze.base.primitives import DirectedAcyclicGraph, StrKey, str_key_selector

from ..planning import OperationPlan
from .resolution import PlanResolution

# ----------------------- #


@final
class RegistryFreezeValidator:
    """Validate handlers, patches, and resolved plans before freeze."""

    @staticmethod
    def validate_all(
        handlers: Mapping[StrKey, HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Run all freeze validations."""

        RegistryFreezeValidator.validate_patches(handlers, resolution)
        RegistryFreezeValidator.validate_resolved_plans(handlers, resolution)
        RegistryFreezeValidator.validate_dispatch_graph(handlers, resolution)

    # ....................... #

    @staticmethod
    def validate_patches(
        handlers: Mapping[StrKey, HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Validate plan patches before freeze."""

        RegistryFreezeValidator._validate_orphan_patches(handlers, resolution)
        RegistryFreezeValidator._validate_patch_specificity_conflicts(
            handlers,
            resolution,
        )

    # ....................... #

    @staticmethod
    def _validate_orphan_patches(
        handlers: Mapping[StrKey, HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Reject patches whose selector matches no registered operation."""

        if not resolution.patches:
            return

        for patch in resolution.patches:
            if not any(str_key_selector.matches(patch.selector, op) for op in handlers):
                raise exc.internal(
                    "Orphan plan patch: selector "
                    f"{patch.selector!r} matches no registered operations"
                )

    # ....................... #

    @staticmethod
    def _validate_patch_specificity_conflicts(
        handlers: Mapping[StrKey, HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Reject equal-specificity patches that cannot merge for the same operation."""

        patches = resolution.patches

        if len(patches) < 2 or not handlers:
            return

        for op in handlers:
            by_specificity: dict[int, list[int]] = defaultdict(list)

            for index in resolution.patch_indices_by_specificity():
                patch = patches[index]

                if not str_key_selector.matches(patch.selector, str(op)):
                    continue

                spec = str_key_selector.specificity(patch.selector)
                by_specificity[spec].append(index)

            for spec, indices in by_specificity.items():
                if len(indices) < 2:
                    continue

                merged = OperationPlan()

                try:
                    for index in indices:
                        merged = merged.merge(patches[index].plan)

                except Exception as e:
                    selectors = tuple(patches[i].selector for i in indices)

                    raise exc.internal(
                        "Conflicting plan patches for operation "
                        f"{op!r} at equal specificity {spec}: "
                        f"selectors {selectors!r}: {e}"
                    ) from e

    # ....................... #

    @staticmethod
    def validate_resolved_plans(
        handlers: Mapping[StrKey, HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Reject resolved plans with transaction stages but no route."""

        for op in handlers:
            plan = resolution.resolve(str(op))

            if plan.tx_requires_route() and plan.tx_route() is None:
                raise exc.internal(
                    f"Operation {op!r} has transaction stages or dispatch "
                    "but no transaction route"
                )

    # ....................... #

    @staticmethod
    def validate_dispatch_graph(
        handlers: Mapping[StrKey, HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Validate the dispatch graph (no loops, all targets registered)."""

        nodes = set(handlers.keys())
        edges: set[tuple[StrKey, StrKey]] = set()

        for op in nodes:
            p = resolution.resolve(str(op))

            for d in p.iter_dispatch():
                if d not in nodes:
                    raise exc.internal(
                        f"Dispatch target {d} not found for operation {op}"
                    )

                edges.add((op, d))

        g = DirectedAcyclicGraph.from_edges(nodes, edges)
        g.validate()
