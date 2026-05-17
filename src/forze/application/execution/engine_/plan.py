from typing import Self, Sequence

import attrs

from forze.base.errors import CoreError
from forze.base.primitives import StrKey

from .slot import MiddlewareSlot
from .specs import MiddlewareSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MiddlewarePlan:
    """Immutable middleware plan for a distinct operation."""

    tx_route: StrKey | None = None
    """Transaction route for this plan."""

    specs: dict[MiddlewareSlot, Sequence[MiddlewareSpec]] = attrs.field(factory=dict)
    """Slot to specs mapping for middlewares in this plan."""

    # ....................... #

    def for_slot(self, slot: MiddlewareSlot) -> Sequence[MiddlewareSpec]:
        return self.specs.get(slot, ())

    # ....................... #

    def add(self, slot: MiddlewareSlot, spec: MiddlewareSpec) -> Self:
        updated = dict(self.specs)
        updated[slot] = (*self.for_slot(slot), spec)

        return attrs.evolve(self, specs=updated)

    # ....................... #

    def enable_tx(self, route: StrKey) -> Self:
        return attrs.evolve(self, tx_route=route)

    # ....................... #

    def disable_tx(self) -> Self:
        return attrs.evolve(self, tx_route=None)

    # ....................... #

    #! TODO: build method

    # ....................... #

    @classmethod
    def merge(cls, *plans: Self, tx_override: bool = False) -> Self:
        """Merge multiple middleware plans into a single plan.

        :param plans: Plans to merge.
        :param tx_override: Allow overriding the transaction route if it conflicts with another plan.
        :returns: New merged plan.

        :raises CoreError: If the transaction routes conflict and ``tx_override`` is :obj:`False`.
        """

        merged_specs: dict[MiddlewareSlot, Sequence[MiddlewareSpec]] = {}
        tx_route: StrKey | None = None

        for plan in plans:
            if plan.tx_route is not None:
                if tx_route is None:
                    tx_route = plan.tx_route

                elif tx_route != plan.tx_route:
                    if tx_override:
                        tx_route = plan.tx_route

                    else:
                        raise CoreError(
                            "Conflicting transaction routes for one operation: "
                            f"{tx_route!r} vs {plan.tx_route!r}",
                        )

            for slot, specs in plan.specs.items():
                if not specs:
                    continue

                merged_specs[slot] = (*merged_specs.get(slot, ()), *specs)

        return cls(tx_route=tx_route, specs=merged_specs)
