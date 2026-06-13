"""Validate the in-memory keyset oracle: traversal order matches an independent sort.

The mock is the canonical keyset oracle the real backends are checked against, so its own
order must be provably correct — not merely self-consistent. Here we compute the expected
order with a plain Python comparator (null = smallest, per-key direction) and assert the
mock's full cursor traversal reproduces it, with single coverage, for multi-key and
mixed-direction sorts.
"""

from __future__ import annotations

from functools import cmp_to_key
from typing import Any

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.cursor_parity import (
    SEED,
    SORT_CASES,
    SORT_CASES_EXPLICIT_NULLS,
    CursorCreate,
    CursorDoc,
    CursorRead,
    _forward_seqs,
    _seq,
    seed_cursor_corpus,
)

pytestmark = pytest.mark.unit


def _mock() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="cur",
        read=CursorRead,
        write=DocumentWriteTypes(domain=CursorDoc, create_cmd=CursorCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="cur",
        read_model=CursorRead,
        domain_model=CursorDoc,
    )


def _expected_seqs(sorts: dict[str, Any]) -> list[int]:
    """Independent expected order: per-key direction with absolute null placement.

    Null placement is the canonical default (asc → first, desc → last) unless the key
    spells out a ``{"dir","nulls"}`` override; the override is *absolute*, independent of
    direction.
    """

    records = [{"grp": c.grp, "score": c.score, "seq": c.seq} for c in SEED]

    def _resolve(value: Any) -> tuple[str, str]:
        if isinstance(value, dict):
            direction = value["dir"]
            nulls = value.get("nulls") or ("first" if direction == "asc" else "last")
            return direction, nulls
        return value, ("first" if value == "asc" else "last")

    def _cmp(a: dict[str, Any], b: dict[str, Any]) -> int:
        for key, value in sorts.items():
            direction, nulls = _resolve(value)
            av, bv = a[key], b[key]

            if av is None and bv is None:
                c = 0
            elif av is None:
                c = -1 if nulls == "first" else 1
            elif bv is None:
                c = 1 if nulls == "first" else -1
            else:
                v = (av > bv) - (av < bv)
                c = -v if direction == "desc" else v

            if c:
                return c

        return 0

    ordered = sorted(records, key=cmp_to_key(_cmp))

    return [r["seq"] for r in ordered]


@pytest.mark.asyncio
@pytest.mark.parametrize("sorts", [*SORT_CASES, *SORT_CASES_EXPLICIT_NULLS])
async def test_mock_traversal_matches_independent_order(sorts: dict[str, Any]) -> None:
    doc = _mock()
    await seed_cursor_corpus(doc)

    got = await _forward_seqs(doc, sorts=sorts, limit=3)

    assert got == _expected_seqs(sorts)
    assert sorted(got) == list(range(len(SEED)))  # every row, exactly once


@pytest.mark.asyncio
@pytest.mark.parametrize("sorts", [*SORT_CASES, *SORT_CASES_EXPLICIT_NULLS])
async def test_offset_and_cursor_order_agree(sorts: dict[str, Any]) -> None:
    # Offset sort and cursor traversal must yield the identical order — same canonical
    # comparison underneath — so switching pagination styles never reorders rows.
    doc = _mock()
    await seed_cursor_corpus(doc)

    cursor_order = await _forward_seqs(doc, sorts=sorts, limit=3)
    page = await doc.find_many(sorts=sorts, pagination={"limit": 100})
    offset_order = [_seq(h) for h in page.hits]

    assert offset_order == cursor_order == _expected_seqs(sorts)
