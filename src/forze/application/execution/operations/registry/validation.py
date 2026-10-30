"""Freeze-time validation for operation registries."""

from collections import defaultdict
from typing import Any, final

from forze.application.contracts.execution import (
    AuthorizesMembers,
    DeclaresAuthz,
    DeclaresHedge,
    HoldsTransactionLock,
    OperationHandlerFactory,
    SharesTransactionLock,
)
from forze.base.exceptions import exc
from forze.base.primitives import (
    DirectedAcyclicGraph,
    StrKey,
    StrKeyMapping,
    str_key_selector,
)

from ..descriptors import OperationDescriptor
from ..planning import OperationPlan
from .resolution import PlanResolution

# ----------------------- #


@final
class RegistryFreezeValidator:
    """Validate handlers, patches, and resolved plans before freeze."""

    @staticmethod
    def validate_all(
        handlers: StrKeyMapping[OperationHandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Run all freeze validations."""

        RegistryFreezeValidator.validate_patches(handlers, resolution)
        RegistryFreezeValidator.validate_tx_locks(handlers, resolution)
        RegistryFreezeValidator.validate_resolved_plans(handlers, resolution)
        RegistryFreezeValidator.validate_dispatch_graph(handlers, resolution)
        RegistryFreezeValidator.validate_hedge_safety(handlers, resolution)
        RegistryFreezeValidator.validate_two_phase(handlers, resolution)

    # ....................... #

    @staticmethod
    def validate_patches(
        handlers: StrKeyMapping[OperationHandlerFactory],
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
        handlers: StrKeyMapping[OperationHandlerFactory],
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
        handlers: StrKeyMapping[OperationHandlerFactory],
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
        handlers: StrKeyMapping[OperationHandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Reject resolved plans with transaction stages or declared isolation but no route."""

        for op in handlers:
            plan = resolution.resolve(str(op))

            if plan.tx_route() is not None:
                continue

            if plan.tx_requires_route():
                raise exc.internal(
                    f"Operation {op!r} has transaction stages or dispatch but no transaction route"
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
    def validate_tx_locks(
        handlers: StrKeyMapping[OperationHandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Refuse a transaction lock that its operation's transaction cannot hold.

        The lock is held until the transaction ends, so its step must run inside it: in the
        transaction scope of an operation with a route. Anywhere else every call would be
        refused. Whether the route's backend has such a lock depends on the wiring, which the
        registry does not see: the step checks it when the operation is resolved, on its first
        call or under ``check_wiring``.

        Operations sharing a lock group must share a route as well: each route's backend holds
        its own locks, so two routes would not exclude each other. Only this registry's
        operations are compared; merge registries before freezing to check across them.
        """

        groups: dict[str, tuple[StrKey, StrKey]] = {}

        for op in handlers:
            plan = resolution.resolve(str(op))

            if any(
                isinstance(step.factory, HoldsTransactionLock)
                for step in plan.iter_outer_before_steps()
            ):
                raise exc.configuration(
                    f"Operation {op!r} takes a transaction lock in its outer scope, which runs "
                    "before the transaction opens; attach it to the transaction scope with "
                    "bind_tx().",
                    code="tx_lock_outside_transaction_scope",
                )

            if plan.tx_route() is None and any(
                isinstance(step.factory, HoldsTransactionLock) for step in plan.iter_before_steps()
            ):
                raise exc.configuration(
                    f"Operation {op!r} takes a transaction lock but has no transaction route, "
                    "so there is no transaction to hold it; bind one via "
                    "bind_tx().set_route(...).",
                    code="tx_lock_without_route",
                )

            if (route := plan.tx_route()) is None:
                continue

            for step in plan.iter_before_steps():
                if not isinstance(step.factory, SharesTransactionLock):
                    continue

                if (group := step.factory.lock_group()) is None:
                    continue

                first_op, first_route = groups.setdefault(group, (op, route))

                if first_route != route:
                    raise exc.configuration(
                        f"Operations {first_op!r} and {op!r} share the transaction lock group "
                        f"{group!r} but run on transaction routes {first_route!r} and "
                        f"{route!r}, whose locks do not exclude each other; bind every "
                        "operation in a group to one route.",
                        code="tx_lock_group_spans_routes",
                    )

    # ....................... #

    @staticmethod
    def validate_hedge_safety(
        handlers: StrKeyMapping[OperationHandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Reject hedged operations whose hedge wraps do not declare an explicit safety basis.

        Hedging fires concurrent duplicate attempts, so it is safe only when the handler
        tolerates them (read-only, OCC, or naturally idempotent) — which the developer
        asserts with ``HedgeWrap(safety=...)``. A boundary ``IdempotencyWrap`` does **not**
        make hedging safe: it is claimed once at the outermost layer (priority 10), *outside*
        the hedge (15), so the hedge's concurrent attempts each open their own transaction
        and can both commit. It dedups separate client requests, not the in-invocation
        concurrent attempts a hedge fires — so it is not accepted as a safety basis here.
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

            if all(h.hedge_safety_declared() for h in hedges):
                continue

            raise exc.configuration(
                f"Operation {op!r} is hedged but no HedgeWrap declares an explicit safety "
                "basis (HedgeWrap(safety=...)). Concurrent duplicate attempts are unsafe "
                "unless the handler tolerates them (read-only, OCC, or naturally "
                "idempotent). A boundary IdempotencyWrap does NOT make hedging safe — it "
                "dedups separate client requests, not the in-invocation concurrent attempts "
                "a hedge fires, which each open their own transaction and can both commit.",
            )

    # ....................... #

    @staticmethod
    def validate_two_phase(
        handlers: StrKeyMapping[OperationHandlerFactory],
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
    def validate_member_authz(
        handlers: StrKeyMapping[OperationHandlerFactory],
        resolution: PlanResolution,
        descriptors: StrKeyMapping[OperationDescriptor],
    ) -> None:
        """Refuse an authz-guarded multi-source operation that leaves a member unauthorized.

        A hub or federated search reads every member's rows under one operation, so a
        permission on the operation alone would reach a member the caller may not read.
        """

        for op in handlers:
            descriptor = descriptors.get(op)

            if descriptor is None or not descriptor.members:
                continue

            plan = resolution.resolve(str(op))
            factories: list[Any] = [step.factory for step in plan.iter_before_steps()]
            factories += [step.factory for step in plan.iter_wrap_steps()]

            if not any(isinstance(factory, DeclaresAuthz) for factory in factories):
                continue

            if plan.supports_idempotency_key():
                # A replayed result answers without running the member check.
                raise exc.configuration(
                    f"Operation {op!r} reads members under authorization but carries a wrap "
                    "that replays stored results; drop the idempotency wrap from it.",
                    code="member_authz_replay",
                )

            covered: set[str] = set()

            for factory in factories:
                if not isinstance(factory, AuthorizesMembers):
                    continue

                # The hook binds its grant under its own search's name, and the adapter reads
                # it under the operation's: another search's hook would leave this one unchecked.
                if factory.members_of() != descriptor.members_of:
                    raise exc.configuration(
                        f"Operation {op!r} reads the members of {descriptor.members_of!r} but "
                        f"carries a member authorization hook for {factory.members_of()!r}; "
                        "attach a hook for the search the operation runs.",
                        code="member_authz_search_mismatch",
                    )

                covered |= factory.authorized_members()

            if missing := sorted(set(descriptor.members) - covered):
                raise exc.configuration(
                    f"Operation {op!r} declares authorization but authorizes no read for its "
                    f"member(s) {missing}; attach AuthzSearchMembers (or another "
                    "AuthorizesMembers hook) naming each member's read action.",
                    code="member_authz_missing",
                )

    # ....................... #

    @staticmethod
    def validate_dispatch_graph(
        handlers: StrKeyMapping[OperationHandlerFactory],
        resolution: PlanResolution,
    ) -> None:
        """Validate the dispatch graph (no loops, all targets registered)."""

        nodes = set(handlers.keys())
        edges: set[tuple[StrKey, StrKey]] = set()

        for op in nodes:
            p = resolution.resolve(str(op))

            for d in p.iter_dispatch():
                if d not in nodes:
                    raise exc.internal(f"Dispatch target {d} not found for operation {op}")

                edges.add((op, d))

        g = DirectedAcyclicGraph.from_edges(nodes, edges)
        g.validate()
