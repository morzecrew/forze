"""Cross-backend parity for a read with no ``limit``: every row once, in one total order.

A read without a limit is drained in batches. Paged by offset, rows that tie on the sort
could land on either side of a batch boundary — a backend is free to order ties differently
from one statement to the next — so one row came back twice and another never. Firestore
refuses an offset past zero outright, so the same read failed on its second batch.

This harness seeds more rows than two batches hold, all sharing a few sort values, and checks
the read against the order the contract promises: the caller's sort, a null sorting as the
smallest value, then ``id`` in the sort's direction (``asc`` when the directions mix). The
mock runs it as the oracle; each real backend runs the same function.
"""

from __future__ import annotations

from collections.abc import Sequence
from functools import cmp_to_key
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class ScanMeta(BaseModel):
    rank: int | None = None  # nested and nullable
    tag: str = ""


class _ScanFields(BaseModel):
    grp: int  # three values across every batch, so ties straddle each boundary
    kind: str  # text, three values
    score: int | None = None  # nullable, ties among the nulls too
    meta: ScanMeta = Field(default_factory=ScanMeta)
    label: str = ""  # mixed case, which a validator can rewrite on read
    email: str = ""  # legacy rows whose domain case was never normalized


class ScanCreate(CreateDocumentCmd, _ScanFields):
    pass


class ScanDoc(Document, _ScanFields):
    pass


class ScanRead(ReadDocument, _ScanFields):
    pass


class ScanIdOnly(BaseModel):
    # Forbids extras: a page read that selects sort-key columns for the seek must not hand
    # them to a model that never asked for them.
    model_config = ConfigDict(extra="forbid")

    id: UUID


class ScanExcluded(BaseModel):
    """Holds every sort key but dumps none of them, so a token built from a dump seeks wrong."""

    id: UUID
    grp: int = Field(exclude=True)
    kind: str = Field(exclude=True)
    score: int | None = Field(default=None, exclude=True)
    meta: ScanMeta = Field(default_factory=ScanMeta, exclude=True)


class ScanScaled(BaseModel):
    """Rewrites the stored ``grp`` on read: a seek from the read value starts past every row."""

    id: UUID
    grp: int

    @field_validator("grp")
    @classmethod
    def _scale(cls, value: int) -> int:
        return value * 10


class ScanLowered(BaseModel):
    """Lowercases the stored ``label`` on read, so it orders differently from the store."""

    id: UUID
    label: str

    @field_validator("label")
    @classmethod
    def _lower(cls, value: str) -> str:
        return value.lower()


class ScanEmail(BaseModel):
    """``EmailStr`` lowercases the domain: legacy mixed-case rows read back changed."""

    id: UUID
    email: EmailStr


class _MetaTagOnly(BaseModel):
    tag: str = ""


class ScanSummary(BaseModel):
    """Carries ``meta`` but not ``meta.rank``: the nested sort key is not in the row."""

    id: UUID
    meta: _MetaTagOnly = Field(default_factory=_MetaTagOnly)


ROWS = 450
"""More rows than two 200-row batches hold."""

POSTGRES_COLUMNS = """
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    grp integer NOT NULL,
    kind text NOT NULL,
    score integer,
    meta jsonb NOT NULL,
    label text NOT NULL,
    email text NOT NULL
"""
"""The table a Postgres leg reads, matching :class:`ScanDoc`."""

SORTS: tuple[dict[str, str], ...] = (
    {"grp": "asc"},
    {"grp": "desc"},
    {"kind": "asc"},
    {"score": "asc"},
    {"score": "desc"},
    {"meta.rank": "asc"},
    {"meta.rank": "desc"},
    {"grp": "asc", "score": "desc"},
)


def seed() -> list[ScanCreate]:
    return [
        ScanCreate(
            grp=i % 3,
            kind=("b", "a", "c")[i % 3],
            score=None if i % 4 == 0 else i % 5,
            meta=ScanMeta(rank=None if i % 5 == 0 else i % 3, tag=f"t{i}"),
            label=("Bx", "ax", "Cx")[i % 3],
            email=f"user{i % 3}@" + ("Example.COM" if i % 2 == 0 else "example.com"),
        )
        for i in range(ROWS)
    ]


def _value(row: Any, key: str) -> Any:
    for part in key.split("."):
        row = getattr(row, part)

    return row


def expected_order(rows: Sequence[Any], sorts: dict[str, str] | None) -> list[UUID]:
    """Row ids in the promised order: each key with null smallest, then ``id``."""

    if sorts is None:
        return sorted(r.id for r in rows)

    directions = set(sorts.values())
    tie = directions.pop() if len(directions) == 1 else "asc"
    keys = [*sorts.items(), ("id", tie)]

    def compare(a: Any, b: Any) -> int:
        for key, direction in keys:
            x, y = _value(a, key), _value(b, key)

            if x == y:
                continue

            c = -1 if x is None else 1 if y is None else (-1 if x < y else 1)

            return c if direction == "asc" else -c

        return 0

    return [r.id for r in sorted(rows, key=cmp_to_key(compare))]


async def run_unbounded_scan_parity(
    command: Any,
    query: Any,
    *,
    custom_sorts: bool = True,
) -> None:
    """Seed :data:`ROWS` rows and read them back without a limit, several ways.

    Each read runs unsorted (``id`` order) and, with *custom_sorts*, under every sort in
    :data:`SORTS`. Pass ``False`` for a backend that cannot page a read sorted by anything
    but ``id`` past one batch (Firestore: its cursor seeks on ``id`` alone and it refuses
    offsets).
    """

    created = await command.create_many(seed())
    cases: list[dict[str, str] | None] = [None]

    if custom_sorts:
        cases += list(SORTS)

    for sorts in cases:
        expected = expected_order(created, sorts)
        label = f"sorts={sorts}"
        roots = sorted({key.split(".", 1)[0] for key in sorts or {}})

        page = await query.find_many(sorts=sorts)
        got = [hit.id for hit in page.hits]

        assert len(set(got)) == ROWS, f"{label}: {ROWS - len(set(got))} rows lost"
        assert got == expected, f"{label}: out of order"

        tail = await query.find_many(sorts=sorts, pagination={"offset": 250})
        assert [hit.id for hit in tail.hits] == expected[250:], label

        counted = await query.find_page(sorts=sorts)
        assert (counted.count, [hit.id for hit in counted.hits]) == (ROWS, expected), label

        projected = await query.project_many(["id", *roots], sorts=sorts)
        assert [UUID(str(row["id"])) for row in projected.hits] == expected, label

        selected = await query.select_many(ScanRead, sorts=sorts)
        assert [row.id for row in selected.hits] == expected, label

        # Every key held, none dumped: the seek reads the fields, not the dump.
        excluded = await query.select_many(ScanExcluded, sorts=sorts)
        assert [row.id for row in excluded.hits] == expected, f"{label}: excluded keys"

        if sorts is not None:
            # A projection or model without the sort key cannot seek; it pages by offset.
            ids_only = await query.project_many(["id"], sorts=sorts)
            assert [UUID(str(row["id"])) for row in ids_only.hits] == expected, label

            narrow = await query.select_many(ScanIdOnly, sorts=sorts)
            assert [row.id for row in narrow.hits] == expected, label

            summary = await query.select_many(ScanSummary, sorts=sorts)
            assert [row.id for row in summary.hits] == expected, f"{label}: nested subset"

    if custom_sorts:
        await _check_rewritten_keys(query)
        await _check_unbounded_aggregate(query)


async def _check_unbounded_aggregate(query: Any) -> None:
    """An aggregate read without a limit pages its groups in the group keys' order.

    One group per row, so the groups outnumber a batch, and every count ties: a caller's
    sort on the count alone leaves the group keys to order them.
    """

    aggregates = {"$groups": {"tag": "meta.tag"}, "$computed": {"n": {"$count": None}}}
    tags = sorted(f"t{i}" for i in range(ROWS))

    for sorts, expected in ((None, tags), ({"n": "desc"}, tags[::-1])):
        page = await query.aggregate_many(aggregates, sorts=sorts)

        assert [row["tag"] for row in page.hits] == expected, f"aggregate sorts={sorts}"


_REWRITTEN: tuple[tuple[type[BaseModel], str], ...] = (
    (ScanScaled, "grp"),
    (ScanLowered, "label"),
    (ScanEmail, "email"),
)


async def _check_rewritten_keys(query: Any) -> None:
    """A model whose validator rewrites a sort key on read still reads every row in order.

    The store orders rows by what it holds; a seek from the rewritten value starts in the
    wrong place. The oracle is the backend's own order for the same sort with ``id`` added,
    read as one bounded page, so a text key's collation is the store's, not Python's.
    """

    for model, key in _REWRITTEN:
        for direction in ("asc", "desc"):
            sorts = {key: direction}
            reference = await query.find_many(
                sorts={key: direction, "id": direction}, pagination={"limit": 1_000}
            )
            expected = [hit.id for hit in reference.hits]

            got = await query.select_many(model, sorts=sorts)
            ids = [row.id for row in got.hits]

            assert len(set(ids)) == len(ids) == ROWS, f"{model.__name__} {sorts}: {len(ids)} rows"
            assert ids == expected, f"{model.__name__} {sorts}: out of order"


async def run_id_first_cursor_parity(command: Any, query: Any) -> None:
    """A cursor sorted by ``id`` first orders by ``id``: the keys after it never decide.

    ``id`` is unique, so ``{"id": "asc", "grp": "desc"}`` is ``id`` order. The keyset used to
    move ``id`` to the end, ordering by ``grp`` first instead.
    """

    created = await command.create_many(seed()[:7])
    ids = sorted(row.id for row in created)

    for sorts, expected in (
        ({"id": "asc", "grp": "desc"}, ids),
        ({"id": "desc", "grp": "asc"}, ids[::-1]),
    ):
        got: list[UUID] = []
        cursor: dict[str, Any] = {"limit": 3}

        while True:
            page = await query.find_cursor(sorts=sorts, cursor=cursor)
            got += [hit.id for hit in page.hits]

            if not page.has_more:
                break

            cursor = {"limit": 3, "after": page.next_cursor}

        assert got == expected, sorts
