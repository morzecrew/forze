"""Reactive cascade topology — who triggers whom, via which domain events.

The operation registries hold opaque callables, so the reactive wiring (saga steps,
domain-event handlers) is invisible to static inspection. But the hardened engine trace
makes it observable at runtime: firing one operation and reading the trace shows every
operation that ran as a *cascade* (B3 op-invokes the harness never drove directly) and
every domain event dispatched along the way (B4 dispatch records). :class:`ReactiveMap`
captures that recovered topology.

It is what powers reactive scenario derivation: the *entry points* (operations triggered by
nothing) are the ones worth driving directly; everything else fires automatically as a
consequence and would be unrealistic to drive standalone.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import cached_property
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class ReactiveMap:
    """The recovered cascade graph: per operation, what it triggers and what it dispatches."""

    cascades: Mapping[str, frozenset[str]]
    """Operation → the operations it triggers reactively (transitively) when fired alone."""

    events: Mapping[str, frozenset[str]]
    """Operation → the domain event types dispatched during its cascade."""

    # ....................... #

    @cached_property
    def reactive_ops(self) -> frozenset[str]:
        """Every operation that is triggered as a cascade of some other operation."""

        triggered: set[str] = set()
        for downstream in self.cascades.values():
            triggered |= downstream

        return frozenset(triggered)

    # ....................... #

    def entry_points(self) -> frozenset[str]:
        """The probed operations that nothing else triggers — the real entry points."""

        reactive = self.reactive_ops

        return frozenset(op for op in self.cascades if op not in reactive)

    # ....................... #

    def triggers(self, op: str) -> frozenset[str]:
        """The operations *op* triggers reactively."""

        return self.cascades.get(op, frozenset())

    # ....................... #

    def format(self) -> str:
        """Render the topology — cascades (with the events that carry them) and entry points."""

        lines = ["reactive topology:"]

        for op in sorted(self.cascades):
            downstream = sorted(self.cascades[op])
            if not downstream:
                continue

            carried = sorted(self.events.get(op, frozenset()))
            via = f" [{', '.join(carried)}]" if carried else ""
            lines.append(f"  {op} →{via} {', '.join(downstream)}")

        if len(lines) == 1:
            lines.append("  (no cascades)")

        lines.append(f"entry points: {', '.join(sorted(self.entry_points())) or '(none)'}")

        return "\n".join(lines)
