"""Tests for :mod:`forze.application.execution.deps.trace`."""

from forze.application.execution.deps import DepsResolutionTrace
from forze.application.execution.deps.resolution import ResolutionFrame, frame_for
from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.document import DocumentQueryDepKey


class TestDepsResolutionTrace:
    def test_add_edge_and_nodes(self) -> None:
        parent = frame_for(CounterDepKey, "route-a")
        child = frame_for(DocumentQueryDepKey, None)
        trace = DepsResolutionTrace()
        trace.add_edge(parent, child)
        trace.add_edge(parent, child)

        nodes = trace.nodes()
        assert parent in nodes
        assert child in nodes
        assert len(trace.edges) == 1

    def test_to_dag_and_canonical_edges(self) -> None:
        a = frame_for(CounterDepKey, None)
        b = frame_for(DocumentQueryDepKey, "docs")
        trace = DepsResolutionTrace(edges={(a, b)})

        dag = trace.to_dag()
        assert a in dag.predecessors
        assert b in dag.predecessors

        canonical = trace.canonical_edges()
        assert (CounterDepKey.name, DocumentQueryDepKey.name) in canonical

        key_dag = trace.to_key_dag()
        assert CounterDepKey.name in key_dag.predecessors

    def test_format_edges(self) -> None:
        a = ResolutionFrame(key_name="parent", route=None)
        b = ResolutionFrame(key_name="child", route="r")
        trace = DepsResolutionTrace(edges={(a, b)})

        text = trace.format_edges()
        assert "parent -> child@r" in text

        canonical_text = trace.format_canonical_edges()
        assert "parent -> child" in canonical_text
