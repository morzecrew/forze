"""Internal dispatch graph for nested usecase execution."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Self, final

import attrs

from forze.base.errors import CoreError
from forze.base.primitives import StrKey

# ----------------------- #


def _qualify(op: StrKey) -> str:  #! re;pc to base primitives types or so
    return str(op)


# ....................... #


@attrs.define(slots=True)
class UsecaseDelegationGraph: ...


# ....................... #


@final
@attrs.define(slots=True)
class DispatchGraph:
    """Directed edges ``(from_op, to_op)`` for synchronous nested dispatch."""

    _edges: set[tuple[str, str]] = attrs.field(factory=set, repr=False)
    _frozen: bool = attrs.field(default=False, repr=False)

    # ....................... #

    @classmethod
    def from_edges(cls, edges: Iterable[tuple[StrKey, StrKey]]) -> Self:
        graph = cls()
        graph.add_edges(edges)

        return graph

    # ....................... #

    def _raise_if_frozen(self) -> None:
        if self._frozen:
            raise CoreError("Registry dispatch graph is frozen")

    # ....................... #

    @property
    def is_frozen(self) -> bool:
        return self._frozen

    # ....................... #

    def freeze(self, *, edges: Iterable[tuple[str, str]] | None = None) -> Self:
        if edges is not None:
            self._edges = set(edges)

        self._frozen = True
        return self

    # ....................... #

    def add_edge(self, from_op: StrKey, to_op: StrKey) -> Self:
        self._raise_if_frozen()
        self._edges.add((_qualify(from_op), _qualify(to_op)))
        return self

    # ....................... #

    def add_edges(self, edges: Iterable[tuple[StrKey, StrKey]]) -> Self:
        self._raise_if_frozen()

        for src, dst in edges:
            self._edges.add((_qualify(src), _qualify(dst)))

        return self

    # ....................... #

    def edges(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._edges)

    # ....................... #

    def has_edge(self, from_op: StrKey, to_op: StrKey) -> bool:
        return (_qualify(from_op), _qualify(to_op)) in self._edges

    # ....................... #

    def outgoing_edges_for(self, op: StrKey) -> frozenset[tuple[str, str]]:
        s = _qualify(op)
        return frozenset((src, dst) for src, dst in self._edges if src == s)

    # ....................... #

    def to_adjacency(self) -> dict[str, frozenset[str]]:
        adj: dict[str, set[str]] = defaultdict(set)

        for src, dst in self._edges:
            adj[src].add(dst)

        return {k: frozenset(v) for k, v in adj.items()}

    # ....................... #

    def merge(self, *others: Self) -> Self:
        out: set[tuple[str, str]] = set(self._edges)

        for other in others:
            out.update(other._edges)

        merged = type(self)()
        merged._edges = out

        return merged
