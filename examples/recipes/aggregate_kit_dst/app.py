"""The moat: declare a governed aggregate → get a **DST-verifiable** slice.

`AggregateKit` composes an aggregate's invariant enforcement into its write ops (preventively,
inside the transaction, at the law's isolation floor). Because the framework *owns* that
composition, the very same `SystemInvariant` you declared compiles into a **DST conformance
oracle** — so a deterministic simulation can prove the enforcement actually holds under
concurrent interleaving, not just single-threaded.

Here every ticket lands in one sprint, so concurrent creates race the capacity cap — the worst
case the invariant must survive. The kit-composed registry runs clean; a *bare* registry with no
enforcement (the test's contrast) breaks under the same schedule. Nothing here is DST-aware except
the `Simulation` — and the oracle is just `compile_oracle(SPRINT_CAPACITY)`.

Try it (from the repo root)::

    forze dst run examples.recipes.aggregate_kit_dst.app:simulation   # ✓ no violation
"""

from __future__ import annotations

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.invariants import ReadSet, SumOf, SystemInvariant
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import Simulation
from forze_dst.oracle import compile_oracle
from forze_kits.aggregates import AggregateKit
from forze_mock import MockDepsModule

# ----------------------- #
# Domain — a sprint's tickets, each carrying a point estimate. Every ticket defaults to one sprint,
# so concurrent creates concentrate on it (the capacity cap is only interesting under contention).


class Ticket(Document):
    sprint_id: str = "S"
    points: int = 1


class TicketCreate(CreateDocumentCmd):
    sprint_id: str = "S"
    points: int = 1


class TicketUpdate(BaseDTO):
    points: int | None = None


class TicketRead(ReadDocument):
    sprint_id: str = "S"
    points: int = 1


TICKET_SPEC = DocumentSpec(
    name="tickets",
    read=TicketRead,
    write=DocumentWriteTypes(
        domain=Ticket, create_cmd=TicketCreate, update_cmd=TicketUpdate
    ),
)

# The cross-record law: a sprint's committed points stay within capacity.
SPRINT_CAPACITY = SystemInvariant(
    name="sprint_capacity",
    read_set=ReadSet(spec=TICKET_SPEC, scope_keys=("sprint_id",)),
    aggregate=SumOf("points"),
    holds=lambda total: total <= 10,
)


# --8<-- [start:kit]
# One declaration → the governed slice. The invariant is enforced preventively on the write ops,
# at its `required_isolation` (SERIALIZABLE by default — enough to defeat write skew).
TICKETS = AggregateKit(spec=TICKET_SPEC, invariants=(SPRINT_CAPACITY,))
# --8<-- [end:kit]


# --8<-- [start:simulation]
# The moat: the same law compiles into a DST oracle. A deterministic simulation drives the kit's
# operations under concurrent interleavings and proves the enforcement holds — no double-book of
# the sprint's capacity. `compile_oracle` is the only bridge; the kit and its models are unchanged.
_ORACLE = compile_oracle(SPRINT_CAPACITY)

simulation = Simulation(
    operations=TICKETS.registry(tx_route="mock"),
    deps=lambda: MockDepsModule(),
    observe=_ORACLE.observe,
    invariants=[*_ORACLE.invariants],
)
# --8<-- [end:simulation]
