"""The graph differential — a bounded neighbourhood, and what a delete takes with it.

Two scripts, chosen because they are the graph plane's two *silent* answers: both return a
plausible-looking result when they are wrong, so neither engine's failure announces itself.

- :func:`run_bounded_neighbors` — a hub whose neighbourhood is larger than the ``limit``
  asked for. What is portable here is the *cardinality*: a bounded call must hand back a
  full page of genuine neighbours, and — when a vertex-kind filter is set — must fill that
  page from the wanted kind rather than spending it on rows it then discards. What is
  **not** portable is *which* neighbours arrive. Neo4j's adjacency ``LIMIT`` carries no
  ``ORDER BY``, so the subset is the planner's business; the mock walks its edge list in
  insertion order. Comparing the identity of the page would be comparing two arbitrary
  choices, so :meth:`BoundedNeighborsOutcome.portable` leaves it out and the divergence is
  catalogued instead.

  The distinction matters more than a page boundary usually does: the rows a filter drops
  are not a cursor the caller can advance past, so a short page reads as "there are no
  more" and a traversal stops early believing it is finished.

- :func:`run_detach_cascade` — deleting a vertex that still has edges. A store that leaves
  the edges behind has dangling adjacency; one that takes too much has silently deleted a
  neighbour's other relationships. The script therefore keeps a bystander edge and a
  self-loop alive across the delete, so "cascaded" and "over-cascaded" land on different
  outcomes instead of both reading as success.

Both are driven through the ports alone, so the same script runs against the in-memory mock
and against a live Neo4j.
"""

from __future__ import annotations

from collections.abc import Awaitable, Sequence
from typing import Any, Protocol

import attrs

from forze.application.contracts.graph import GraphDirection, VertexRef

# ----------------------- #

BOUNDED_LIMIT = 2
"""Rows a bounded call asks for. Smaller than either kind's population on purpose."""

WANTED_NEIGHBORS = 3
"""Neighbours of the kind the caller wants — more than :data:`BOUNDED_LIMIT`, so the page
is genuinely truncated and the engine has to choose."""

UNWANTED_PER_SIDE = BOUNDED_LIMIT
"""Neighbours of the excluded kind seeded *before* and *again after* the wanted ones.

Both the count and the placement are load-bearing, and getting the placement wrong is how
this probe silently stops probing. A limit-then-filter implementation spends its budget on
whichever rows its engine reads first, so the excluded kind has to be there — and the two
engines read from opposite ends. Neo4j hands back adjacency most-recent-first; the mock
walks its edge list in insertion order. Padding one end catches one engine and lets the
other fill the page by luck, which is exactly what an earlier version of this scenario did:
a mock mutated to limit before filtering still passed. Padding both ends, with at least
``BOUNDED_LIMIT`` on each, leaves no order in which the fault is invisible.
"""

UNWANTED_NEIGHBORS = UNWANTED_PER_SIDE * 2
"""Total neighbours of the excluded kind."""


# ....................... #


class GraphReads(Protocol):
    """The reads these scripts need from a graph query port."""

    def neighbors(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
        to_vertex_kinds: frozenset[str] | None = None,
    ) -> Awaitable[Sequence[Any]]: ...  # pragma: no cover

    def count_edges(self, edge_kind: str) -> Awaitable[int]: ...  # pragma: no cover

    def vertex_exists(self, ref: VertexRef) -> Awaitable[bool]: ...  # pragma: no cover

    def vertex_degree(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
    ) -> Awaitable[int]: ...  # pragma: no cover


class GraphWrites(Protocol):
    """The writes these scripts need from a graph command port."""

    def create_vertex(
        self,
        node_kind: str,
        cmd: Any,
        *,
        return_new: bool = True,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def create_edge(self, edge_kind: str, cmd: Any) -> Awaitable[Any]: ...  # pragma: no cover

    def delete_vertex(self, ref: VertexRef) -> Awaitable[None]: ...  # pragma: no cover


class GraphFixture(Protocol):
    """Builds the spec-shaped command payloads a script needs, per engine-agnostic script.

    The scripts know the *shape* of the graph they want, not the caller's DTOs — every
    model is application-defined, so the leg supplies them and the script stays portable.
    """

    def vertex(self, kind: str, key: str) -> Any: ...  # pragma: no cover

    def edge(
        self,
        *,
        from_kind: str,
        from_key: str,
        to_kind: str,
        to_key: str,
    ) -> Any: ...  # pragma: no cover


# ....................... #


@attrs.frozen(kw_only=True)
class BoundedNeighborsOutcome:
    """What a truncated neighbourhood read answered."""

    page_size: int
    """Rows returned for an unfiltered call with ``limit`` below the neighbour count.

    Must equal the limit: a bounded read is allowed to truncate, never to under-deliver.
    """

    filtered_page_size: int
    """The same call with a vertex-kind filter, the excluded kind outnumbering the limit.

    Must also equal the limit. Anything less is the limit having been applied before the
    filter — the shortfall a caller reads as "there are no more".
    """

    filtered_all_wanted: bool
    """Whether every row of the filtered page really is of the requested kind."""

    filtered_page_is_subset: bool
    """Whether the filtered page is drawn from the true neighbour set.

    The weaker half of the identity question, and the only half that is portable: *which*
    of the wanted neighbours arrive is the engine's choice, but they must be neighbours.
    """

    unbounded_total: int
    """Neighbours reachable with a generous limit — the control.

    Without it, a page of ``limit`` rows proves nothing: an engine with only ``limit``
    neighbours in the first place would pass every assertion above.
    """

    page_ids: tuple[str, ...]
    """The identities the filtered page came back with, in arrival order.

    Recorded, never compared — see the module docstring. Kept in the outcome so a leg can
    log what an engine chose, and so the catalogued divergence has a place to point.
    """

    def portable(self) -> tuple[int, int, bool, bool, int]:
        """The part every graph store can honestly promise, and the only part compared."""

        return (
            self.page_size,
            self.filtered_page_size,
            self.filtered_all_wanted,
            self.filtered_page_is_subset,
            self.unbounded_total,
        )


@attrs.frozen(kw_only=True)
class CascadeOutcome:
    """What deleting a still-connected vertex took with it."""

    edges_before: int
    """Edges in place before the delete — the control that the graph was really built."""

    edges_after: int
    """Edges left afterwards. The deleted vertex's own edges must be gone, and only those."""

    deleted_is_gone: bool
    """The vertex itself is no longer there."""

    neighbor_survives: bool
    """Its neighbour is still a vertex — a cascade that removed it went too far."""

    neighbor_degree: int
    """The neighbour's remaining degree: its edge to the deleted vertex is gone, and the
    bystander edge it holds to a third vertex is not."""

    self_loop_survives: bool
    """An unrelated vertex's ``a → a`` edge, untouched.

    A self-loop is the case both implementations are most likely to mishandle, because it
    is the one edge where a vertex is its own neighbour in every direction.
    """


# ----------------------- #

EXPECTED_BOUNDED_NEIGHBORS_PORTABLE = (
    BOUNDED_LIMIT,
    BOUNDED_LIMIT,
    True,
    True,
    WANTED_NEIGHBORS + UNWANTED_NEIGHBORS,
)
"""A full page both times, every row genuine, and the neighbourhood really is larger."""


EXPECTED_CASCADE = CascadeOutcome(
    edges_before=3,
    edges_after=2,
    deleted_is_gone=True,
    neighbor_survives=True,
    neighbor_degree=1,
    self_loop_survives=True,
)
"""One edge removed, two kept: the bystander edge and the self-loop.

Every plausible mistake lands somewhere else. Leaving the edges behind reads ``3``;
cascading through the neighbour reads ``0`` and drops ``neighbor_survives``; taking the
self-loop with it reads ``1``.
"""


# ----------------------- #


async def run_bounded_neighbors(
    commands: GraphWrites,
    reads: GraphReads,
    fixture: GraphFixture,
    *,
    hub_kind: str,
    wanted_kind: str,
    unwanted_kind: str,
    edge_kind: str,
) -> BoundedNeighborsOutcome:
    """Build an over-populated neighbourhood and read it back under a limit."""

    await commands.create_vertex(hub_kind, fixture.vertex(hub_kind, "hub"))

    wanted_keys: list[str] = []

    async def _attach(kind: str, key: str) -> None:
        await commands.create_vertex(kind, fixture.vertex(kind, key))
        await commands.create_edge(
            edge_kind,
            fixture.edge(from_kind=kind, from_key=key, to_kind=hub_kind, to_key="hub"),
        )

    # Excluded kind, wanted kind, excluded kind again — see UNWANTED_PER_SIDE. Whichever
    # end an engine reads from, it meets a full limit's worth of rows it must discard
    # before reaching a single row it may keep.
    for index in range(UNWANTED_PER_SIDE):
        await _attach(unwanted_kind, f"u{index}")

    for index in range(WANTED_NEIGHBORS):
        key = f"w{index}"
        wanted_keys.append(key)
        await _attach(wanted_kind, key)

    for index in range(UNWANTED_PER_SIDE, UNWANTED_NEIGHBORS):
        await _attach(unwanted_kind, f"u{index}")

    hub = VertexRef(kind=hub_kind, key="hub")
    edges = frozenset({edge_kind})

    unfiltered = await reads.neighbors(hub, GraphDirection.IN, edges, limit=BOUNDED_LIMIT)
    filtered = await reads.neighbors(
        hub,
        GraphDirection.IN,
        edges,
        limit=BOUNDED_LIMIT,
        to_vertex_kinds=frozenset({wanted_kind}),
    )
    unbounded = await reads.neighbors(
        hub,
        GraphDirection.IN,
        edges,
        limit=(WANTED_NEIGHBORS + UNWANTED_NEIGHBORS) * 2,
    )

    page_ids = tuple(str(row.other.id) for row in filtered)

    return BoundedNeighborsOutcome(
        page_size=len(unfiltered),
        filtered_page_size=len(filtered),
        filtered_all_wanted=all(key.startswith("w") for key in page_ids),
        filtered_page_is_subset=set(page_ids) <= set(wanted_keys),
        unbounded_total=len(unbounded),
        page_ids=page_ids,
    )


async def run_detach_cascade(
    commands: GraphWrites,
    reads: GraphReads,
    fixture: GraphFixture,
    *,
    vertex_kind: str,
    edge_kind: str,
) -> CascadeOutcome:
    """Delete a vertex that still has an edge, with a bystander and a self-loop watching."""

    for key in ("doomed", "neighbor", "third", "loop"):
        await commands.create_vertex(vertex_kind, fixture.vertex(vertex_kind, key))

    def _link(from_key: str, to_key: str) -> Any:
        return fixture.edge(
            from_kind=vertex_kind,
            from_key=from_key,
            to_kind=vertex_kind,
            to_key=to_key,
        )

    await commands.create_edge(edge_kind, _link("doomed", "neighbor"))
    # The bystander: the neighbour keeps this one, so an over-eager cascade is visible.
    await commands.create_edge(edge_kind, _link("neighbor", "third"))
    await commands.create_edge(edge_kind, _link("loop", "loop"))

    edges_before = await reads.count_edges(edge_kind)

    await commands.delete_vertex(VertexRef(kind=vertex_kind, key="doomed"))

    return CascadeOutcome(
        edges_before=edges_before,
        edges_after=await reads.count_edges(edge_kind),
        deleted_is_gone=not await reads.vertex_exists(VertexRef(kind=vertex_kind, key="doomed")),
        neighbor_survives=await reads.vertex_exists(VertexRef(kind=vertex_kind, key="neighbor")),
        neighbor_degree=await reads.vertex_degree(
            VertexRef(kind=vertex_kind, key="neighbor"),
            direction=GraphDirection.BOTH,
        ),
        self_loop_survives=await reads.vertex_degree(
            VertexRef(kind=vertex_kind, key="loop"),
            direction=GraphDirection.OUT,
        )
        > 0,
    )
