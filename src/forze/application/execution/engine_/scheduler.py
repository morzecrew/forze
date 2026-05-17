from graphlib import CycleError, TopologicalSorter
from typing import Self, Sequence

import attrs

from forze.base.errors import CoreError
from forze.base.primitives import StrKey

from .plan import MiddlewarePlan
from .slot import MiddlewareSlot
from .specs import MiddlewareSpec

# ----------------------- #


@attrs.define(slots=True)
class CapabilityScheduler:
    """Schedules capability steps for one slot."""

    specs: Sequence[MiddlewareSpec]
    """Specs to schedule."""

    slot: MiddlewareSlot
    """Slot to schedule (only for logging / tracing purposes)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.slot.is_schedulable():
            raise CoreError(
                f"Slot {self.slot.value} is not schedulable",
            )

    # ....................... #

    @classmethod
    def from_plan(cls, plan: MiddlewarePlan, slot: MiddlewareSlot) -> Self:
        return cls(slot=slot, specs=plan.specs.get(slot, ()))

    # ....................... #

    def _derive_providers(self) -> dict[StrKey, int]:
        """Derive a mapping of capability keys to the index of the step that provides them."""

        providers: dict[StrKey, int] = {}

        for idx, spec in enumerate(self.specs):
            for key in spec.provides:
                if key in providers:
                    raise CoreError(
                        f"Capability {key!r} is provided by more than one step in slot {self.slot.value} (indices {providers[key]} and {idx})",
                    )

                providers[key] = idx

        return providers

    # ....................... #

    def _derive_graph(self, providers: dict[StrKey, int]) -> dict[int, set[int]]:
        """Derive graph in TopologicalSorter format: node -> dependencies."""

        graph: dict[int, set[int]] = {idx: set() for idx in range(len(self.specs))}

        for idx, spec in enumerate(self.specs):
            for key in spec.requires:
                provider_idx = providers.get(key)

                if provider_idx is None:
                    raise CoreError(
                        f"Capability {key!r} is required by a step in slot {self.slot.value} but no step in this slot provides it",
                    )

                if provider_idx == idx:
                    raise CoreError(
                        f"Step at index {idx} both requires and provides {key!r}",
                    )

                graph[idx].add(provider_idx)

        return graph

    # ....................... #

    def _derive_topological_order(self, graph: dict[int, set[int]]) -> list[int]:
        """Derive a topological order of steps."""

        sorter = TopologicalSorter(graph)

        try:
            sorter.prepare()

        except CycleError as e:
            raise CoreError(
                f"Capability dependency graph in slot {self.slot.value} "
                f"contains a cycle",
            ) from e

        order: list[int] = []

        while sorter.is_active():
            ready = sorted(
                sorter.get_ready(),
                key=lambda idx: (-self.specs[idx].priority, idx),
            )

            for idx in ready:
                order.append(idx)
                sorter.done(idx)

        return order

    # ....................... #

    def _derive(self) -> list[int]:
        """Derive a topological order of steps."""

        providers = self._derive_providers()
        graph = self._derive_graph(providers)
        return self._derive_topological_order(graph)

    # ....................... #

    def schedule(self) -> Sequence[MiddlewareSpec]:
        if not self.specs:
            return self.specs

        if not any(spec.requires or spec.provides for spec in self.specs):
            return self.specs

        order = self._derive()

        return tuple(self.specs[idx] for idx in order)
