"""Unit tests for :mod:`forze.base.primitives.graph`."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives.graph import DirectedAcyclicGraph, build_predecessor_map


def test_from_edges_topological_order() -> None:
    g = DirectedAcyclicGraph.from_edges({"a", "b", "c"}, {("a", "b"), ("b", "c")})
    order = g.topological_order(ready_sort_key=str)
    assert order.index("a") < order.index("b") < order.index("c")


def test_validate_rejects_cycle() -> None:
    g = DirectedAcyclicGraph(predecessors={"a": {"c"}, "b": {"a"}, "c": {"b"}})
    with pytest.raises(CoreException, match="cycle"):
        g.validate()


def test_static_order_matches_batches() -> None:
    g = DirectedAcyclicGraph.from_edges({"x", "y"}, {("x", "y")})
    assert g.static_order() == ("x", "y")
    batches = list(g.topological_batches())
    assert batches == [("x",), ("y",)]


def test_build_predecessor_map_rejects_unknown_node() -> None:
    with pytest.raises(CoreException, match="unknown node"):
        build_predecessor_map({"a"}, {("a", "b")})


def test_build_predecessor_map_rejects_self_loop() -> None:
    with pytest.raises(CoreException, match="self-loop"):
        build_predecessor_map({"a"}, {("a", "a")})
