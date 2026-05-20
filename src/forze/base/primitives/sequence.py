from __future__ import annotations

from typing import Self

import attrs

from forze.base.descriptors import hybridmethod

# ----------------------- #
#! Maybe add unique: bool or dedupe callable


@attrs.define(slots=True, kw_only=True, frozen=True)
class AbstractSequence[X]:
    """Typed sequence of abstract items with merging capabilities."""

    items: tuple[X, ...] = attrs.field(factory=tuple)
    """Items for this sequence."""

    # ....................... #

    def add(self, *items: X) -> Self:
        """Add items to this sequence."""

        return attrs.evolve(self, items=(*self.items, *items))

    # ....................... #

    @hybridmethod
    def merge[V](cls: type[AbstractSequence[V]], *sequences: AbstractSequence[V]) -> AbstractSequence[V]:  # type: ignore[misc, override]
        """Merge multiple sequences into a single sequence."""

        merged_items: tuple[V, ...] = ()

        for sequence in sequences:
            merged_items = (*merged_items, *sequence.items)

        res = cls(items=merged_items)

        return res

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: AbstractSequence[X], *sequences: AbstractSequence[X]) -> AbstractSequence[X]:  # type: ignore[misc, override]
        return type(self).merge(self, *sequences)
