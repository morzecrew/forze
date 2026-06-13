"""Cross-backend keyset (cursor) pagination parity: real backends must match the oracle.

The in-memory mock is the canonical keyset oracle (``compare_keyset_sort_values``): a
null sort value is the smallest value (``asc`` → nulls first, ``desc`` → nulls last) and
each key is compared in its own direction, so mixed ``asc``/``desc`` orders are stable.

This harness seeds a fixed corpus with **nullable** sort keys (to catch a backend whose
seek silently drops null-keyed rows) and runs multi-key, mixed-direction sorts. Every
sort case ends in the unique ``seq`` key, so the order is total and the ``id``
tie-breaker never has to disambiguate — letting the mock and a real backend agree exactly
despite generating different ids. A full forward traversal must (a) reproduce the oracle
order and (b) cover every row exactly once; a ``before`` page must round-trip.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _CursorFields(BaseModel):
    grp: int  # low-cardinality non-null key — drives mixed-direction grouping
    score: int | None = None  # NULLABLE key — the null-ordering / no-drop probe
    seq: int  # unique 0..N-1 — final total-order tie-breaker (never null)


class CursorCreate(CreateDocumentCmd, _CursorFields):
    pass


class CursorDoc(Document, _CursorFields):
    pass


class CursorRead(ReadDocument, _CursorFields):
    pass


# Fixed seed: 12 rows, three groups, several null scores, duplicate scores within a
# group (so the mixed sort genuinely interleaves), unique seq.
SEED: tuple[CursorCreate, ...] = (
    CursorCreate(grp=0, score=5, seq=0),
    CursorCreate(grp=0, score=None, seq=1),
    CursorCreate(grp=0, score=2, seq=2),
    CursorCreate(grp=1, score=None, seq=3),
    CursorCreate(grp=1, score=9, seq=4),
    CursorCreate(grp=1, score=2, seq=5),
    CursorCreate(grp=2, score=7, seq=6),
    CursorCreate(grp=2, score=None, seq=7),
    CursorCreate(grp=0, score=9, seq=8),
    CursorCreate(grp=2, score=2, seq=9),
    CursorCreate(grp=1, score=5, seq=10),
    CursorCreate(grp=0, score=2, seq=11),
)

# Each case ends in ``seq`` (unique, non-null) → total order, id tie-break never fires.
SORT_CASES: tuple[dict[str, Any], ...] = (
    {"score": "asc", "seq": "asc"},  # nulls first (canonical for asc)
    {"score": "desc", "seq": "asc"},  # nulls last (canonical for desc)
    {"grp": "asc", "score": "desc", "seq": "asc"},  # MIXED + nullable middle key
    {"grp": "desc", "score": "asc", "seq": "desc"},  # MIXED, other way
    {"seq": "desc"},  # single desc key
)

# Explicit per-key NULLS FIRST/LAST overrides — the opposite of the canonical default,
# so a backend that ignored the override would mis-place the null-scored rows.
SORT_CASES_EXPLICIT_NULLS: tuple[dict[str, Any], ...] = (
    {"score": {"dir": "asc", "nulls": "last"}, "seq": "asc"},  # asc but nulls LAST
    {"score": {"dir": "desc", "nulls": "first"}, "seq": "asc"},  # desc but nulls FIRST
    {
        "grp": "asc",
        "score": {"dir": "asc", "nulls": "last"},
        "seq": "asc",
    },  # mixed-ish with an override on the nullable key
)


# ....................... #


def _seq(hit: Any) -> int:
    return int(hit["seq"] if isinstance(hit, dict) else hit.seq)


async def seed_cursor_corpus(port: Any) -> None:
    """Create every seed row through *port* (a create + find_cursor capable port)."""

    for cmd in SEED:
        await port.create(cmd)


async def _forward_seqs(port: Any, *, sorts: dict[str, str], limit: int) -> list[int]:
    """Walk all forward pages, returning the full ordered list of ``seq`` values."""

    seqs: list[int] = []
    after: str | None = None
    guard = 0

    while True:
        cursor: dict[str, Any] = {"limit": limit}

        if after is not None:
            cursor["after"] = after

        page = await port.find_cursor(filters=None, cursor=cursor, sorts=sorts)
        seqs.extend(_seq(h) for h in page.hits)

        if not page.next_cursor:
            break

        after = page.next_cursor
        guard += 1

        if guard > 1000:
            raise AssertionError(f"cursor pagination did not terminate for {sorts}")

    return seqs


async def assert_cursor_parity(
    real_port: Any,
    oracle_port: Any,
    *,
    limit: int = 3,
) -> None:
    """Seed both ports, then assert *real_port* matches the *oracle_port* on every case.

    Both ports must already be empty; this seeds each with :data:`SEED`. For each sort
    case it checks order parity, full single-cover (no dropped/duplicated rows), and a
    ``before`` page round-trip.
    """

    await seed_cursor_corpus(oracle_port)
    await seed_cursor_corpus(real_port)

    n = len(SEED)

    for sorts in (*SORT_CASES, *SORT_CASES_EXPLICIT_NULLS):
        oracle = await _forward_seqs(oracle_port, sorts=sorts, limit=limit)
        real = await _forward_seqs(real_port, sorts=sorts, limit=limit)

        assert real == oracle, f"order mismatch for {sorts}:\n real={real}\n oracle={oracle}"
        assert sorted(real) == list(range(n)), (
            f"coverage gap for {sorts}: {sorted(real)} (a null-keyed row may be dropped)"
        )

        # ``before`` round-trip: page 2's prev cursor must page back to page 1.
        p1 = await real_port.find_cursor(
            filters=None, cursor={"limit": limit}, sorts=sorts
        )

        if not p1.next_cursor:
            continue

        p2 = await real_port.find_cursor(
            filters=None,
            cursor={"limit": limit, "after": p1.next_cursor},
            sorts=sorts,
        )

        if not p2.prev_cursor:
            continue

        back = await real_port.find_cursor(
            filters=None,
            cursor={"limit": limit, "before": p2.prev_cursor},
            sorts=sorts,
        )

        assert [_seq(h) for h in back.hits] == [_seq(h) for h in p1.hits], (
            f"before round-trip mismatch for {sorts}"
        )
