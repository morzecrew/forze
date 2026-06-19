"""Freeze-time validation for operation registries."""

from collections import defaultdict
from typing import final

from forze.application.contracts.execution import (
    DeclaresHedge,
    HandlerFactory,
)
from forze.base.exceptions import exc
from forze.base.primitives import (
    DirectedAcyclicGraph,
    StrKey,
    StrKeyMapping,
    str_key_selector,
)

from ..planning import OperationPlan
from .resolution import PlanResolution

# ----------------------- #


@final
class RegistryFreezeValidator:
    """Validate handlers, patches, and resolved plans before freeze."""

    @staticmethod
    def validate_all(
        handlers: StrKeyMapping[HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Run all freeze validations."""

        RegistryFreezeValidator.validate_patches(handlers, resolution)
        RegistryFreezeValidator.validate_resolved_plans(handlers, resolution)
        RegistryFreezeValidator.validate_dispatch_graph(handlers, resolution)
        RegistryFreezeValidator.validate_hedge_safety(handlers, resolution)
        RegistryFreezeValidator.validate_two_phase(handlers, resolution)

    # ....................... #

    @staticmethod
    def validate_patches(
        handlers: StrKeyMapping[HandlerFactory],
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
        handlers: StrKeyMapping[HandlerFactory],
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
        handlers: StrKeyMapping[HandlerFactory],
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
        handlers: StrKeyMapping[HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Reject resolved plans with transaction stages or declared isolation but no route."""

        for op in handlers:
            plan = resolution.resolve(str(op))

            if plan.tx_route() is not None:
                continue

            if plan.tx_requires_route():
                raise exc.internal(
                    f"Operation {op!r} has transaction stages or dispatch "
                    "but no transaction route"
                )

            # Declaring an isolation level without a transaction route would run the operation
            # non-transactionally and silently drop the requirement — fail closed instead.
            isolation = plan.tx_isolation()

            if isolation is not None:
                raise exc.configuration(
                    f"Operation {op!r} declares isolation={isolation.name} but no "
                    "transaction route (set_isolation requires a bound tx route via "
                    "set_route); isolation cannot be honored without a transaction.",
                )

    # ....................... #

    @staticmethod
    def validate_hedge_safety(
        handlers: StrKeyMapping[HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Reject hedged operations that are neither idempotency-guarded nor declared safe.

        Hedging fires concurrent duplicate attempts, so it is only safe on
        idempotent / read-only operations. An operation carrying a hedge wrap passes
        only if it also carries an idempotency guard (auto-detected) or every hedge
        wrap declares an explicit safety basis.
        """

        for op in handlers:
            plan = resolution.resolve(str(op))

            hedges = [
                step.factory
                for step in plan.iter_wrap_steps()
                if isinstance(step.factory, DeclaresHedge)
            ]

            if not hedges:
                continue

            # Shared structural ProvidesIdempotency detection — the same derivation
            # the catalog's ``supports_idempotency_key`` flag uses.
            if plan.supports_idempotency_key() or all(
                h.hedge_safety_declared() for h in hedges
            ):
                continue

            raise exc.configuration(
                f"Operation {op!r} is hedged but has no idempotency guard and no "
                "explicit HedgeWrap(safety=...); concurrent duplicates are unsafe.",
            )

    # ....................... #

    @staticmethod
    def validate_two_phase(
        handlers: StrKeyMapping[HandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Validate two-phase (``prepare``/``apply``) operations.

        A two-phase operation needs a transaction route — the whole point is to run
        ``prepare`` outside the transaction and ``apply`` inside it. (``prepare``
        runs exactly once per invocation even under retry/hedge, so no re-run
        safety declaration is needed.)
        """

        for op in handlers:
            plan = resolution.resolve(str(op))

            if not plan.two_phase:
                continue

            if plan.tx_route() is None:
                raise exc.configuration(
                    f"Operation {op!r} is two-phase (prepare/apply) but has no "
                    "transaction route; bind one via bind_tx().set_route(...). "
                    "Two-phase runs apply inside a transaction.",
                )

    # ....................... #

    @staticmethod
    def validate_dispatch_graph(
        handlers: StrKeyMapping[HandlerFactory],
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
