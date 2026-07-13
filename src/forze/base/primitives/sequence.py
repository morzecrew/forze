from __future__ import annotations

from collections.abc import Iterator
from typing import Self

import attrs

from forze.base.descriptors import hybridmethod

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AbstractSequence[X]:
    """Typed sequence of items with merging capabilities."""

    items: tuple[X, ...] = attrs.field(factory=tuple)
    """Items for this sequence."""

    # ....................... #

    def add(self, *items: X) -> Self:
        """Add items to this sequence."""

        return attrs.evolve(self, items=(*self.items, *items))

    # ....................... #

    @hybridmethod
    def merge[V](
        cls: type[AbstractSequence[V]],  # type: ignore[misc, override]
        *sequences: AbstractSequence[V],
    ) -> AbstractSequence[V]:
        """Merge multiple sequences into a single sequence."""

        merged_items: tuple[V, ...] = ()

        for sequence in sequences:
            merged_items = (*merged_items, *sequence.items)

        res = cls(items=merged_items)

        return res

    # ....................... #

    @merge.instancemethod  # type: ignore[arg-type]
    def _merge_instance(  # type: ignore[misc, override]
        self: AbstractSequence[X],
        *sequences: AbstractSequence[X],
    ) -> AbstractSequence[X]:
        return type(self).merge(self, *sequences)

    # ....................... #

    def __iter__(self) -> Iterator[X]:
        return iter(self.items)
