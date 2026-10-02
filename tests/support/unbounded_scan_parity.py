"""Cross-backend parity for a read with no ``limit``: every row once, in one total order.

A read without a limit is drained in batches. Paged by offset, rows that tie on the sort
could land on either side of a batch boundary — a backend is free to order ties differently
from one statement to the next — so one row came back twice and another never. Firestore
refuses an offset past zero outright, so the same read failed on its second batch.

This harness seeds more rows than two batches hold, all sharing three sort values, and checks
the read against the order the contract promises: the caller's sort, then ``id`` in the
sort's direction. The mock runs it as the oracle; each real backend runs the same function.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _ScanFields(BaseModel):
    grp: int  # three values across every batch, so ties straddle each boundary
    label: str = ""


class ScanCreate(CreateDocumentCmd, _ScanFields):
    pass


class ScanDoc(Document, _ScanFields):
    pass


class ScanRead(ReadDocument, _ScanFields):
    pass


class ScanIdOnly(BaseModel):
    id: UUID


ROWS = 450
"""More rows than two 200-row batches hold."""


def _expected(rows: Sequence[Any], sorts: dict[str, str] | None) -> list[UUID]:
    if sorts is None:
        return sorted(r.id for r in rows)

    ordered = sorted(rows, key=lambda r: (r.grp, r.id), reverse=sorts["grp"] == "desc")

    return [r.id for r in ordered]


async def run_unbounded_scan_parity(
    command: Any,
    query: Any,
    *,
    custom_sorts: bool = True,
) -> None:
    """Seed :data:`ROWS` rows and read them back without a limit, several ways.

    Each read runs unsorted (``id`` order) and, with *custom_sorts*, sorted by the tied
    ``grp`` both ways. Pass ``False`` for a backend that cannot page a read sorted by
    anything but ``id`` past one batch (Firestore: its cursor seeks on ``id`` alone and it
    refuses offsets).
    """

    created = await command.create_many(
        [ScanCreate(grp=i % 3, label=f"row-{i}") for i in range(ROWS)]
    )
    cases: list[dict[str, str] | None] = [None]

    if custom_sorts:
        cases += [{"grp": "asc"}, {"grp": "desc"}]

    for sorts in cases:
        expected = _expected(created, sorts)
        label = f"sorts={sorts}"

        page = await query.find_many(sorts=sorts)
        got = [hit.id for hit in page.hits]

        assert len(set(got)) == ROWS, f"{label}: {ROWS - len(set(got))} rows lost"
        assert got == expected, f"{label}: ties out of id order"

        tail = await query.find_many(sorts=sorts, pagination={"offset": 250})
        assert [hit.id for hit in tail.hits] == expected[250:], label

        counted = await query.find_page(sorts=sorts)
        assert (counted.count, [hit.id for hit in counted.hits]) == (ROWS, expected), label

        projected = await query.project_many(["id", "grp"], sorts=sorts)
        assert [UUID(str(row["id"])) for row in projected.hits] == expected, label

        selected = await query.select_many(ScanRead, sorts=sorts)
        assert [row.id for row in selected.hits] == expected, label

        if sorts is not None:
            # A projection without the sort key cannot seek; it pages by offset, still in order.
            ids_only = await query.project_many(["id"], sorts=sorts)
            assert [UUID(str(row["id"])) for row in ids_only.hits] == expected, label

            narrow = await query.select_many(ScanIdOnly, sorts=sorts)
            assert [row.id for row in narrow.hits] == expected, label
