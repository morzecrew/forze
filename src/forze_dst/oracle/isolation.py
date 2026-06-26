"""Transactional-isolation oracles — snapshot-isolation and serializability over read/write sets.

The register-history checkers ([linearizability.py](linearizability.py)) reason about a *single*
object. The transactional anomalies — lost update, write skew — are defined over a transaction's
**read set** and **write set**, so they need a per-transaction view the id-only register history
does not give.

This module is the **sound kernel** of that check: pure functions over explicit
:class:`TxRecord`s. Two transactions are **concurrent** when their sequence spans overlap; a
transaction **committed** when its outcome is success. Over the concurrent, committed transactions:

- :func:`find_snapshot_isolation_violations` flags a **write-write** conflict (two concurrent
  committers wrote a common key) — the lost update snapshot isolation forbids;
- :func:`find_serializable_violations` additionally flags a **write-skew** anti-dependency (each
  read a key the other wrote) — so its violations are a superset (``serializable ⊋ snapshot``).

Both pairwise checks are **sound** (a flagged set genuinely violates the level) but **incomplete**:
they catch the canonical two-transaction key-level anomalies, not anti-dependency cycles spanning
three or more transactions. For serializability, :func:`find_serializability_cycle` is the
**complete** check (sound *and* complete for conflict-serializability): it builds the dependency
serialization graph — edges directed by the entity ``rev`` version order — and reports a cycle,
catching the ≥3-transaction anti-dependency cycles (e.g. the read-only anomaly) the pairwise check
cannot. It needs the value-trace (``rev`` per call), so it runs only under ``capture_values``. It also
adds **predicate (phantom) edges**: a scan records its captured filter, and a concurrent committed
write whose produced row satisfies that filter — but which the scan provably did not see (it committed
later in trace order) — is a predicate anti-dependency, evaluated with the one shared DSL matcher so
the oracle's predicate semantics match the backend's exactly.

**Feeding the kernel.** A correct per-transaction read/write set requires attributing each port call
to the *transaction* that issued it — which operation spans cannot give, since concurrent operations
interleave at every ``await`` and a span-based attribution misattributes an outer call that lands
inside an inner span. So the trace stamps a run-global ``tx_id`` on every event:
:func:`transactions_from_history` groups by it (sound, exact), and :func:`snapshot_isolation` /
:func:`serializable` wrap the kernel as ``History``-reading :data:`Invariant`s for a sweep. A
transaction **committed** iff its scope emitted a ``tx`` ``exit`` event with ``outcome == "commit"``
— the exit fires from a ``finally`` on commit *and* rollback, so the outcome (not the bare event)
is the commit signal; its concurrency window is the span of trace sequences carrying its ``tx_id``.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping, Sequence, cast, final

import attrs

from forze.application.contracts.querying import evaluate_filter
from forze.application.execution.tracing.port_proxy import REDACTED
from forze.base.exceptions import CoreException, exc
from forze_dst.oracle.invariants import Invariant, Violation
from forze_dst.oracle.recorder import History

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class TxRecord:
    """One transaction's read/write sets + commit outcome — the kernel's unit of analysis.

    ``start``/``end`` are a sequence interval defining the transaction's concurrency window (two
    transactions are concurrent iff their intervals overlap); ``reads``/``writes`` the entity keys
    it read/wrote; ``committed`` whether it succeeded.
    """

    name: str
    start: int
    end: int
    committed: bool
    reads: frozenset[Any]
    writes: frozenset[Any]


# ....................... #


def _concurrent(a: TxRecord, b: TxRecord) -> bool:
    """Whether two transactions' concurrency intervals overlap (they interleaved)."""

    return a.start <= b.end and b.start <= a.end


def _committed_concurrent_pairs(
    txns: Sequence[TxRecord],
) -> list[tuple[TxRecord, TxRecord]]:
    committed = [tx for tx in txns if tx.committed]

    return [
        (committed[i], committed[j])
        for i in range(len(committed))
        for j in range(i + 1, len(committed))
        if _concurrent(committed[i], committed[j])
    ]


def _keys(keys: frozenset[Any]) -> str:
    return ", ".join(sorted(str(key) for key in keys))


# ....................... #


def find_snapshot_isolation_violations(txns: Sequence[TxRecord]) -> list[Violation]:
    """No two concurrent committed transactions wrote a common key (no lost update).

    Snapshot isolation aborts the second committer on a write-write conflict (first-committer-wins),
    so two concurrent transactions that both committed a write to the same key is a lost update SI
    forbids. Sound and incomplete: catches key-level write-write conflicts, not predicate phantoms.
    Disjoint-write anomalies (write skew) are *permitted* under SI and are not flagged here — see
    :func:`find_serializable_violations`.
    """

    return [
        Violation(
            invariant="snapshot_isolation",
            message=(
                f"concurrent committed transactions {a.name!r} and {b.name!r} both wrote "
                f"key(s) {{{_keys(a.writes & b.writes)}}} — a lost update snapshot isolation forbids"
            ),
        )
        for a, b in _committed_concurrent_pairs(txns)
        if a.writes & b.writes
    ]


def find_serializable_violations(txns: Sequence[TxRecord]) -> list[Violation]:
    """The committed transactions admit a serial order (no lost update, no write skew).

    Over each concurrent committed pair, flags either a **write-write** conflict (a lost update —
    also non-serializable) or a **write-skew** anti-dependency, where each transaction read a key
    the other wrote (the canonical snapshot-permits / serializable-forbids anomaly). Strictly
    stronger than :func:`find_snapshot_isolation_violations` (its violations are a superset). Sound
    and incomplete: catches the two-transaction anomalies, not predicate phantoms or anti-dependency
    cycles spanning three or more transactions.
    """

    violations: list[Violation] = []

    for a, b in _committed_concurrent_pairs(txns):
        if write_write := a.writes & b.writes:
            violations.append(
                Violation(
                    invariant="serializable",
                    message=(
                        f"concurrent committed transactions {a.name!r} and {b.name!r} both wrote "
                        f"key(s) {{{_keys(write_write)}}} — a lost update, not serializable"
                    ),
                )
            )
        elif (a.reads & b.writes) and (b.reads & a.writes):
            violations.append(
                Violation(
                    invariant="serializable",
                    message=(
                        f"concurrent committed transactions {a.name!r} and {b.name!r} form a "
                        f"write-skew anti-dependency: each read a key the other wrote "
                        f"({a.name!r} read {{{_keys(a.reads & b.writes)}}}, "
                        f"{b.name!r} read {{{_keys(b.reads & a.writes)}}}) — not serializable"
                    ),
                )
            )

    return violations


# ....................... #
# The COMPLETE kernel — conflict-serializability via the dependency serialization graph (DSG).
# Nodes are committed transactions; a directed edge a→b means a must precede b in any equivalent
# serial order (ww/wr/rw conflicts). A history is conflict-serializable IFF the graph is acyclic, so
# a cycle is the violation — sound AND complete (the pairwise checker above is the 2-cycle case).
# Edges are directed by the entity ``rev`` each call observed/produced: ``rev`` IS the per-key
# version order, so wr/rw directions are exact, not guessed from commit timing.


@final
@attrs.define(frozen=True, kw_only=True)
class ScanRead:
    """One predicate (scan) read — the unit of the phantom-edge construction (Phase 2).

    A scan reads *the set of rows matching a predicate over a namespace* rather than a single keyed
    version, so it carries no ``(key, rev)``. ``namespace`` is the scanned route (the spec name);
    ``predicate`` is the captured filter (``None`` = match-all); ``seq`` is the scan call event's
    trace sequence — the happened-before reference that directs the anti-dependency exactly: a writer
    that committed *after* ``seq`` provably could not have been seen by this scan. ``predicate`` is
    excluded from equality/hash (a mapping is unhashable, and the record is never hashed by value).
    """

    namespace: Any
    predicate: Mapping[str, Any] | None = attrs.field(eq=False)
    seq: int


@final
@attrs.define(frozen=True, kw_only=True)
class WriteVersion:
    """One produced row version — carried so a concurrent scan's predicate can be tested against it.

    ``key`` is the ``(namespace, id)`` pair and ``rev`` the produced revision (as in the keyed write
    set); ``row`` is the redaction-applied dump of the written entity, evaluated against a scanner's
    filter to decide a *predicate-precise* phantom edge (a namespace-coarse rule would false-positive).
    ``row`` is excluded from equality/hash (it is a mapping and the record is never hashed by value).
    """

    key: Any
    rev: int
    row: Mapping[str, Any] = attrs.field(eq=False)


@final
@attrs.define(frozen=True, kw_only=True)
class VersionedTxRecord:
    """A transaction's *version-aware* read/write sets — the unit of the DSG.

    Each ``reads``/``writes`` entry is ``(key, rev)``: the entity revision the call observed (reads)
    or produced (writes). The ``rev`` makes the dependency edges between transactions directable
    exactly by version order. ``scans`` are this transaction's predicate reads and ``write_rows`` its
    produced rows — together they let the graph add predicate (phantom) anti-dependency edges
    (Phase 2): a scan whose predicate a concurrent committed write satisfies, where the write was not
    visible to the scan. Both default empty, so a capture without scan data builds the item-level
    graph exactly as before.
    """

    name: str
    start: int
    end: int
    committed: bool
    commit_seq: int | None
    reads: frozenset[tuple[Any, int]]
    writes: frozenset[tuple[Any, int]]
    scans: tuple[ScanRead, ...] = ()
    write_rows: tuple[WriteVersion, ...] = ()


def _find_cycle(
    nodes: set[str], edges: Mapping[tuple[str, str], str]
) -> list[str] | None:
    """Return one directed cycle (a node ring) in the graph, or ``None`` if acyclic.

    Iterative DFS (an explicit stack, so a long dependency chain can't hit Python's recursion limit),
    with both the start order (``sorted(nodes)``) and each node's out-edges sorted, so the reported
    cycle is deterministic regardless of hash seed.
    """

    adjacency: dict[str, list[str]] = defaultdict(list)
    for source, target in edges:
        adjacency[source].append(target)
    for out_edges in adjacency.values():
        out_edges.sort()

    white, gray, black = 0, 1, 2
    color: dict[str, int] = dict.fromkeys(nodes, white)

    for root in sorted(nodes):
        if color[root] != white:
            continue

        path: list[str] = [root]
        stack: list[tuple[str, int]] = [(root, 0)]
        color[root] = gray

        while stack:
            node, index = stack[-1]

            if index >= len(adjacency[node]):
                stack.pop()
                path.pop()
                color[node] = black
                continue

            stack[-1] = (node, index + 1)
            nxt = adjacency[node][index]

            if color.get(nxt, white) == gray:  # back edge → cycle from nxt..node
                return path[path.index(nxt) :]

            if color.get(nxt, white) == white:
                color[nxt] = gray
                path.append(nxt)
                stack.append((nxt, 0))

    return None


def _has_redaction(value: Any) -> bool:
    """Whether a captured value (a predicate, recursively) contains the redaction mask.

    A predicate over a declared-sensitive field captures the masked value on both sides, so evaluating
    it would compare ``"<redacted>"`` to ``"<redacted>"`` and *manufacture* a match — a false-positive
    edge. The oracle refuses to reason over such a predicate (a false-negative, which is sound).
    """

    if isinstance(value, str):
        return value == REDACTED

    if isinstance(value, Mapping):
        mapping = cast("Mapping[Any, Any]", value)  # type: ignore[redundant-cast]
        return any(_has_redaction(item) for item in mapping.values())

    if isinstance(value, (list, tuple)):
        return any(_has_redaction(item) for item in cast("Sequence[Any]", value))

    return False


def _row_matches(predicate: Mapping[str, Any] | None, row: Mapping[str, Any]) -> bool:
    """Whether *row* satisfies *predicate*, via the one shared DSL evaluator (so oracle ≡ backend).

    A ``None`` predicate is match-all (a scan with no filter). A predicate the evaluator cannot parse
    (it should always be a valid captured filter, but defend anyway) yields ``False`` — no edge — so a
    malformed predicate is a false-negative, never a false-positive.
    """

    try:
        return evaluate_filter(dict(row), cast("Any", predicate))

    except CoreException:
        return False


def _predicate_edges(
    committed: Sequence[VersionedTxRecord],
) -> list[tuple[str, str, str]]:
    """The predicate (phantom) anti-dependency edges over the committed transactions (Phase 2).

    For each scanner ``S`` of predicate ``P`` over namespace ``N`` at scan sequence ``s``, and each
    *other* committed writer ``W`` whose commit sequence is **after** ``s`` (so ``S`` provably did not
    see ``W``'s write — no dirty reads, per the conformance horizon), emit ``S → W`` (``rw``) when ``W``
    produced a row in ``N`` that satisfies ``P``. Directed by trace order (``commit_seq`` vs ``seq``)
    rather than a per-key ``rev``, because a predicate read spans a set, not one version — and that
    order is exact. Predicate-precise (the captured row is matched against ``P``): a namespace-coarse
    rule would false-positive on a write that does not match. A scan whose predicate touches a redacted
    field is skipped (sound). The *disappearance* phantom (a write moves a row out of ``P``) needs no
    rule here — the scan captured the old matching row as a hit, so it is already a keyed ``rw`` edge.
    Iteration is sorted so the reported counterexample is deterministic across hash seeds.
    """

    edges: list[tuple[str, str, str]] = []

    for scanner in sorted(committed, key=lambda tx: tx.name):
        for scan in sorted(scanner.scans, key=lambda sc: sc.seq):
            if _has_redaction(scan.predicate):
                continue

            for writer in sorted(committed, key=lambda tx: tx.name):
                if writer.name == scanner.name or writer.commit_seq is None:
                    continue

                if writer.commit_seq <= scan.seq:
                    continue  # committed at/before the scan → may have been seen → skip (false-neg)

                for wv in writer.write_rows:
                    if wv.key[0] == scan.namespace and _row_matches(
                        scan.predicate, wv.row
                    ):
                        edges.append(
                            (
                                scanner.name,
                                writer.name,
                                f"rw predicate {scan.namespace}@{scan.seq} matched "
                                f"{wv.key[1]}@{wv.rev}",
                            )
                        )
                        break  # one matching row establishes the S→W edge

    return edges


def find_serializability_cycle(txns: Sequence[VersionedTxRecord]) -> list[Violation]:
    """A dependency cycle over the committed transactions — a complete conflict-serializability check.

    Builds the DSG from the version-aware read/write sets — ``ww`` along each key's committed version
    chain, ``wr`` from a version's writer to its reader, ``rw`` from a reader to the writer of the
    next version (the direct anti-dependency, per Adya's MVSG) — plus **predicate (phantom) edges**
    (:func:`_predicate_edges`): a scan whose captured filter a concurrent committed write satisfies,
    where the write committed after the scan (so the scan could not have seen it) — and reports the
    first cycle. Sound and **complete for conflict-serializability over the captured history**: it
    catches write skew (a 2-cycle of anti-dependencies), the three-transaction read-only anomaly (a
    3-cycle), read-modify-write lost update, and predicate phantoms — anything the pairwise checker
    misses beyond two transactions. A ``key`` is the ``(namespace, id)`` pair, so distinct documents
    that share an id across specs do not conflate. Honest bounds, all **false-negative only** (a missing
    edge never adds a false cycle, so soundness is preserved): (1) only point reads (``get``) and
    ``find_many``-family *hits* are versioned reads; a ``count``/``exists`` predicate read contributes a
    forward phantom edge but, returning no rows, cannot supply the reverse ``wr`` edge that would close
    a two-transaction count-phantom cycle, and ``find_stream`` captures no hits; (2) a predicate over a
    declared-sensitive (redacted) field is skipped (it cannot be soundly evaluated); (3) hard deletes
    and ``return_diff`` writes are not captured as versions. Only as sound as the backend's ``rev`` /
    isolation fidelity — the conformance horizon the adapter differential verifies.
    """

    committed = [tx for tx in txns if tx.committed]

    writer_of: dict[tuple[Any, int], str] = {}
    revs_by_key: dict[Any, set[int]] = defaultdict(set)

    for tx in committed:
        for key, rev in tx.writes:
            writer_of[(key, rev)] = tx.name
            revs_by_key[key].add(rev)

    ordered_revs = {key: sorted(revs) for key, revs in revs_by_key.items()}
    edges: dict[tuple[str, str], str] = {}

    def add_edge(source: str, target: str, label: str) -> None:
        if source != target:
            edges.setdefault((source, target), label)

    # Sorted iteration keeps the reported counterexample deterministic across hash seeds.
    for key, revs in sorted(ordered_revs.items(), key=lambda item: repr(item[0])):
        for earlier, later in zip(revs, revs[1:]):  # ww: along the version chain
            add_edge(
                writer_of[(key, earlier)],
                writer_of[(key, later)],
                f"ww {key}@{earlier}→{later}",
            )

    for tx in committed:
        for key, rev in sorted(tx.reads, key=repr):
            if (writer := writer_of.get((key, rev))) is not None:
                add_edge(
                    writer, tx.name, f"wr {key}@{rev}"
                )  # read this writer's version

            following = next((r for r in ordered_revs.get(key, ()) if r > rev), None)
            if (
                following is not None
            ):  # rw: anti-depend on whoever overwrote what we read
                add_edge(
                    tx.name, writer_of[(key, following)], f"rw {key}@{rev}→{following}"
                )

    # Predicate (phantom) anti-dependency edges (Phase 2): a scan whose predicate a concurrent
    # committed write satisfies, where the write was not visible to the scan (committed after it).
    for source, target, label in _predicate_edges(committed):
        add_edge(source, target, label)

    cycle = _find_cycle({tx.name for tx in committed}, edges)

    if cycle is None:
        return []

    ring = " → ".join([*cycle, cycle[0]])
    labels = ", ".join(
        edges[(cycle[i], cycle[(i + 1) % len(cycle)])] for i in range(len(cycle))
    )

    return [
        Violation(
            invariant="serializable",
            message=(
                f"non-serializable: committed transactions form a dependency cycle "
                f"{ring} ({labels})"
            ),
        )
    ]


# ....................... #
# Feeding the kernel from a recorded DST history (sound, via the per-event tx_id seam).


@final
@attrs.define
class _TxBuilder:
    """Mutable accumulator for one transaction's trace facts, frozen into a :class:`TxRecord`."""

    start: int
    end: int
    committed: bool = False
    reads: set[Any] = attrs.field(factory=set)
    writes: set[Any] = attrs.field(factory=set)


def transactions_from_history(
    history: History,
    *,
    read_phase: str = "query",
    write_phase: str = "command",
) -> list[TxRecord]:
    """Derive per-transaction read/write sets from a recorded history, grouped by ``tx_id``.

    Sound because grouping is by the run-global transaction id the trace stamps on every event, not
    by operation span (which misattributes interleaved calls). A transaction's concurrency window is
    the span of trace sequences carrying its id; it ``committed`` iff a ``tx`` ``exit`` event was
    recorded under it (a clean commit). Keyed calls are classified by ``phase``. Events without a
    ``tx_id`` (outside any transaction, or an untraced/production run) are ignored.
    """

    builders: dict[int, _TxBuilder] = {}

    for event in history.of_kind("trace"):
        fields = event.fields
        tx_id = fields.get("tx_id")

        if tx_id is None:
            continue

        seq = int(fields.get("trace_seq", -1))
        builder = builders.get(tx_id)

        if builder is None:
            builders[tx_id] = builder = _TxBuilder(start=seq, end=seq)
        else:
            builder.start = min(builder.start, seq)
            builder.end = max(builder.end, seq)

        if (
            fields.get("trace_domain") == "tx"
            and fields.get("op") == "exit"
            and fields.get("outcome") == "commit"
        ):
            builder.committed = True

        key = fields.get("key")
        phase = fields.get("phase")

        # Key on (namespace, id): two documents sharing an id across specs must not conflate into one
        # version chain (which would manufacture a spurious cross-spec conflict).
        if key is not None and phase == read_phase:
            builder.reads.add((fields.get("route"), key))
        elif key is not None and phase == write_phase:
            builder.writes.add((fields.get("route"), key))

    return [
        TxRecord(
            name=f"tx{tx_id}",
            start=builder.start,
            end=builder.end,
            committed=builder.committed,
            reads=frozenset(builder.reads),
            writes=frozenset(builder.writes),
        )
        for tx_id, builder in sorted(builders.items())
    ]


# Query-port ops that read by *predicate* (a scanned set), not by a single key — so the call carries
# a filter (the captured ``payload``) and a phantom edge may run against it. ``get``/``get_many`` are
# point reads and excluded; ``find_stream`` records only its call (an async generator), so it captures
# the predicate but no hits.
_PREDICATE_READ_OPS = frozenset(
    {"find", "find_many", "find_page", "find_cursor", "find_stream", "count", "exists"}
)


@final
@attrs.define
class _VersionedTxBuilder:
    """Mutable accumulator for one transaction's version-aware facts (see :class:`VersionedTxRecord`)."""

    start: int
    end: int
    committed: bool = False
    commit_seq: int | None = None
    reads: set[tuple[Any, int]] = attrs.field(factory=set)
    writes: set[tuple[Any, int]] = attrs.field(factory=set)
    scans: list[ScanRead] = attrs.field(factory=list)
    write_rows: list[WriteVersion] = attrs.field(factory=list)


def versioned_transactions_from_history(
    history: History, *, read_phase: str = "query", write_phase: str = "command"
) -> list[VersionedTxRecord]:
    """Derive version-aware per-transaction read/write sets from the value-trace, grouped by ``tx_id``.

    Reads the capture-mode *result* events: each carries the entity ``id`` + ``rev``, so a write's
    produced version and a read's observed version are exact — and a ``create`` (whose call has no
    leading-id ``key``) is recorded from its result like any other write, unlike the key-only
    :func:`transactions_from_history`. A key is the ``(namespace, id)`` pair (the namespace is the
    trace's ``route`` = the spec name), so documents that share an id across specs never conflate.
    Requires ``SimulationConfig.capture_values``; **fails closed** (``exc.configuration``) if the run
    issued document writes but captured none, so running the complete check without the value-trace
    raises instead of passing vacuously (an empty graph is trivially acyclic). Predicate reads (a scan
    op's call event) are recorded as ``scans`` — the captured filter + the scan sequence — for phantom
    edges, and each produced write keeps its full row (``write_rows``) so a scan's filter can be tested
    against it; ``find_many``-family hits arrive as their own result events and fold into the keyed read
    set (a re-scan that sees a concurrent insert thus also yields the keyed ``wr``). See
    :func:`find_serializability_cycle` for the bounds.
    """

    builders: dict[int, _VersionedTxBuilder] = {}
    saw_document_write = False
    captured_writes = 0

    for event in history.of_kind("trace"):
        fields = event.fields
        tx_id = fields.get("tx_id")

        if tx_id is None:
            continue

        seq = int(fields.get("trace_seq", -1))
        builder = builders.get(tx_id)

        if builder is None:
            builders[tx_id] = builder = _VersionedTxBuilder(start=seq, end=seq)
        else:
            builder.start = min(builder.start, seq)
            builder.end = max(builder.end, seq)

        if (
            fields.get("trace_domain") == "tx"
            and fields.get("op") == "exit"
            and fields.get("outcome") == "commit"
        ):
            builder.committed = True
            builder.commit_seq = seq

        phase = fields.get("phase")
        op = fields.get("op")
        raw_result = fields.get("result")

        # A predicate-read CALL event (a scan op, query phase, no single-entity result on this event):
        # record the captured filter as a scan over its namespace for phantom edges. ``find_many``'s
        # hits arrive as their own result events below and fold into the keyed read set as usual.
        if (
            phase == read_phase
            and op in _PREDICATE_READ_OPS
            and not isinstance(raw_result, Mapping)
        ):
            predicate = fields.get("payload")
            builder.scans.append(
                ScanRead(
                    namespace=fields.get("route"),
                    predicate=(
                        cast("Mapping[str, Any]", predicate)
                        if isinstance(predicate, Mapping)
                        else None
                    ),
                    seq=seq,
                )
            )
            continue

        # A document write was *issued* (capture-on additionally emits a result event below); used to
        # tell "capture off" (writes issued, none captured) from "no writes" (read-only → serializable).
        if phase == write_phase and fields.get("trace_domain") == "document":
            saw_document_write = True

        if not isinstance(raw_result, Mapping):
            continue

        result = cast("Mapping[str, Any]", raw_result)
        rid, rev = result.get("id"), result.get("rev")

        if (
            rid is None or rev is None
        ):  # not a single-entity result (e.g. a scan page / count) — skip
            continue

        key = (
            fields.get("route"),
            rid,
        )  # (namespace, id): never conflate ids across specs

        if phase == read_phase:
            builder.reads.add((key, int(rev)))
        elif phase == write_phase:
            builder.writes.add((key, int(rev)))
            builder.write_rows.append(WriteVersion(key=key, rev=int(rev), row=result))
            captured_writes += 1

    if saw_document_write and captured_writes == 0:
        raise exc.configuration(
            "serializable(complete=True) needs the value-trace to read entity revisions — run with "
            "SimulationConfig(capture_values=True). The history issued document writes but captured "
            "none, so the dependency graph would be vacuously empty.",
            code="serializability_graph_requires_capture",
        )

    return [
        VersionedTxRecord(
            name=f"tx{tx_id}",
            start=builder.start,
            end=builder.end,
            committed=builder.committed,
            commit_seq=builder.commit_seq,
            reads=frozenset(builder.reads),
            writes=frozenset(builder.writes),
            scans=tuple(builder.scans),
            write_rows=tuple(builder.write_rows),
        )
        for tx_id, builder in sorted(builders.items())
    ]


def snapshot_isolation(
    *, read_phase: str = "query", write_phase: str = "command"
) -> Invariant:
    """An :data:`Invariant`: the run's committed transactions are snapshot-isolated (no lost update).

    Derives per-transaction read/write sets from the trace (by ``tx_id``) and applies
    :func:`find_snapshot_isolation_violations`. Declarable in any sweep config.
    """

    def _check(history: History) -> list[Violation]:
        return find_snapshot_isolation_violations(
            transactions_from_history(
                history, read_phase=read_phase, write_phase=write_phase
            )
        )

    return _check


def serializable(
    *, complete: bool = False, read_phase: str = "query", write_phase: str = "command"
) -> Invariant:
    """An :data:`Invariant`: the run's committed transactions are serializable.

    Two modes. The default (``complete=False``) is the lightweight **pairwise** check
    (:func:`find_serializable_violations`) — capture-free, sound but incomplete: it flags lost update
    and write skew (two-transaction anomalies). ``complete=True`` is the **dependency-graph** check
    (:func:`find_serializability_cycle`) — sound *and* complete for conflict-serializability over the
    captured history, catching anti-dependency cycles spanning three or more transactions (e.g. the
    read-only anomaly) via the entity ``rev`` version order, and predicate **phantoms** via the
    captured scan filter. The complete mode reads the value-trace, so it requires
    ``SimulationConfig.capture_values`` (and fails closed without it); see
    :func:`find_serializability_cycle` for the false-negative-only bounds.
    """

    def _check(history: History) -> list[Violation]:
        if complete:
            return find_serializability_cycle(
                versioned_transactions_from_history(
                    history, read_phase=read_phase, write_phase=write_phase
                )
            )

        return find_serializable_violations(
            transactions_from_history(
                history, read_phase=read_phase, write_phase=write_phase
            )
        )

    return _check
