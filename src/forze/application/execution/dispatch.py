"""Usecase dispatch graph validation (static cycle detection)."""

from __future__ import annotations

from collections import defaultdict

from forze.base.errors import CoreError

# ----------------------- #


def format_dispatch_cycle(path: tuple[str, ...]) -> str:
    """Format a cycle path for error messages."""

    return " -> ".join(path)


# ....................... #


def find_dispatch_cycle(edges: frozenset[tuple[str, str]]) -> tuple[str, ...] | None:
    """Return a cycle as a tuple of operation keys, or ``None``.

    Directed graph: edges are ``(source, target)``. Uses DFS with a recursion
    stack and reports the first cycle found.
    """

    if not edges:
        return None

    adj: defaultdict[str, list[str]] = defaultdict(list)
    all_nodes: set[str] = set()

    for src, dst in edges:
        adj[src].append(dst)
        all_nodes.add(src)
        all_nodes.add(dst)

    visited: set[str] = set()
    rec_stack: set[str] = set()
    path: list[str] = []

    def dfs(node: str) -> tuple[str, ...] | None:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)

        for nb in adj.get(node, ()):
            if nb not in visited:
                cyc = dfs(nb)
                if cyc is not None:
                    return cyc

            elif nb in rec_stack:
                idx = path.index(nb)
                return (*path[idx:], nb)

        path.pop()
        rec_stack.discard(node)
        return None

    for n in sorted(all_nodes):
        if n not in visited:
            cyc = dfs(n)
            if cyc is not None:
                return cyc

    return None


# ....................... #


def assert_dispatch_graph_acyclic(edges: frozenset[tuple[str, str]]) -> None:
    """Raise :exc:`~forze.base.errors.CoreError` when the edge set has a cycle."""

    cyc = find_dispatch_cycle(edges)

    if cyc is None:
        return

    raise CoreError(
        "Usecase dispatch graph contains a cycle: "
        f"{format_dispatch_cycle(cyc)}. "
        "Declare dispatch edges only from parent operations to strictly "
        "downstream operations, or remove redundant edges."
    )


# ....................... #


def assert_dispatch_edges_reference_registered_ops(
    edges: frozenset[tuple[str, str]],
    registered_ops: set[str],
) -> None:
    """Raise when an edge references an operation that is not registered."""

    for src, dst in edges:
        if src not in registered_ops:
            raise CoreError(
                f"Dispatch edge source operation {src!r} is not registered"
            )

        if dst not in registered_ops:
            raise CoreError(
                f"Dispatch edge target operation {dst!r} is not registered"
            )


# ....................... #


def expand_wildcard_dispatch_sources(
    edges: frozenset[tuple[str, str]],
    registered_ops: set[str],
    *,
    wildcard: str = "*",
) -> frozenset[tuple[str, str]]:
    """Replace ``(wildcard, dst)`` with one edge per registered operation.

    Middleware specs for the base plan use source ``"*"`` (:data:`WILDCARD` in
    :mod:`forze.application.execution.plan`); this expands them to
    ``(op, dst)`` for every registered ``op`` except ``op == dst`` so a global
    “run ``dst`` after every op” pattern does not add a trivial self-edge.
    """

    if not registered_ops:
        return frozenset(e for e in edges if e[0] != wildcard)

    out: set[tuple[str, str]] = set()

    for src, dst in edges:
        if src == wildcard:
            for op in registered_ops:
                if op != dst:
                    out.add((op, dst))
        else:
            out.add((src, dst))

    return frozenset(out)
