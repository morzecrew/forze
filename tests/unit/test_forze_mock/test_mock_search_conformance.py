"""The in-memory oracle against the shared search battery.

The oracle searches the mock document store, so its corpus arrives through the document
adapter — the same shape as Postgres and Mongo searching their system of record.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchResultSnapshotSpec, SearchSpec
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockSearchAdapter, MockState
from forze_mock.adapters.search.command import (
    MockSearchCommandAdapter,
    MockSearchManagementAdapter,
)
from forze_mock.adapters.search.snapshot import MockSearchResultSnapshotAdapter
from tests.support.search_conformance import (
    CORPUS,
    DEFAULT_SORT,
    SEARCH_BATTERY,
    SEARCH_WRITE_BATTERY,
    Check,
    SearchHarness,
    SearchWriteHarness,
    WriteCheck,
    searchable_fields,
)

pytestmark = pytest.mark.asyncio


class _Row(BaseModel):
    id: UUID
    title: str
    content: str
    category: str = ""
    price: Decimal = Decimal(0)
    rank: int | None = None


class _Domain(Document):
    title: str
    content: str
    category: str = ""
    price: Decimal = Decimal(0)
    rank: int | None = None


class _Read(ReadDocument):
    title: str
    content: str
    category: str = ""
    price: Decimal = Decimal(0)
    rank: int | None = None


class _Create(CreateDocumentCmd):
    title: str
    content: str
    category: str = ""
    price: Decimal = Decimal(0)
    rank: int | None = None


class _Update(BaseDTO):
    title: str | None = None


@pytest_asyncio.fixture
async def harness() -> SearchHarness:
    state = MockState()
    documents = MockDocumentAdapter(
        spec=DocumentSpec(
            name="rows",
            read=_Read,
            write=DocumentWriteTypes(
                domain=_Domain,
                create_cmd=_Create,
                update_cmd=_Update,
            ),
        ),
        state=state,
        namespace="rows",
        read_model=_Read,
        domain_model=_Domain,
    )

    for title, content, category, price, rank in CORPUS:
        await documents.create(
            _Create(title=title, content=content, category=category, price=price, rank=rank)
        )

    return SearchHarness(
        query=MockSearchAdapter(
            state=state,
            spec=SearchSpec(
                name="rows",
                model_type=_Row,
                fields=searchable_fields(),
                facetable_fields=frozenset({"category"}),
                default_sort=DEFAULT_SORT,
            ),
        ),
        backend="mock",
        # The oracle reads a blank query as "filter-only over everything".
        blank_query_matches_all=True,
    )


@pytest.mark.conformance(plane="search", engine="mock")
@pytest.mark.parametrize("check", SEARCH_BATTERY, ids=lambda check: check.__name__)
async def test_search_battery(check: Check, harness: SearchHarness) -> None:
    await check(harness)


async def test_relevance_orders_before_the_sort(harness: SearchHarness) -> None:
    """The oracle ranks as the real engines do: by score first, then by the request's sort.

    Postgres and Mongo put the rank ahead of the sort keys, and Meilisearch's default ranking
    rules put word matches ahead of ``sort``. An oracle that sorted first would answer a
    ranked page in an order no backend gives.
    """

    page = await harness.query.search("python notes", None, {"limit": 50}, {"title": "asc"})

    assert [hit.title for hit in page.hits] == [
        "delta notes",
        "gamma notes",
        "alpha guide",
        "beta guide",
        "manual",
        "the unabridged manual",
    ]


# ....................... #


@pytest.fixture
def write_harness() -> SearchWriteHarness:
    state = MockState()
    spec = SearchSpec(name="rows", model_type=_Row, fields=searchable_fields())

    return SearchWriteHarness(
        command=MockSearchCommandAdapter(state=state, spec=spec),
        management=MockSearchManagementAdapter(state=state, spec=spec),
        query=MockSearchAdapter(state=state, spec=spec),
        backend="mock",
        new_row=lambda title: _Row(id=uuid4(), title=title, content="python"),
    )


@pytest.mark.conformance(plane="search_write", engine="mock")
@pytest.mark.parametrize("check", SEARCH_WRITE_BATTERY, ids=lambda check: check.__name__)
async def test_search_write_battery(check: WriteCheck, write_harness: SearchWriteHarness) -> None:
    await check(write_harness)


async def test_a_snapshot_replays_only_for_the_order_it_was_taken_in() -> None:
    """A snapshot taken under another default sort is not replayed: the page runs live."""

    state = MockState()
    documents = MockDocumentAdapter(
        spec=DocumentSpec(
            name="rows",
            read=_Read,
            write=DocumentWriteTypes(domain=_Domain, create_cmd=_Create, update_cmd=_Update),
        ),
        state=state,
        namespace="rows",
        read_model=_Read,
        domain_model=_Domain,
    )
    rows = [
        await documents.create(_Create(title=title, content=content, category=category))
        for title, content, category, *_ in CORPUS
    ]
    rs_spec = SearchResultSnapshotSpec(name="snap", enabled=True)
    store = MockSearchResultSnapshotAdapter(state=state, spec=rs_spec)

    # Taken ascending, under the request alone: what the key held before it took the order in.
    ascending = sorted(rows, key=lambda row: row.title)
    stale = SearchResultSnapshot.simple_search_fingerprint(
        "python", None, None, spec_name="rows", variant="offset"
    )
    await store.put_run(
        run_id="run-1",
        fingerprint=stale,
        ordered_ids=[
            SearchResultSnapshot.result_record_key_string(_Row.model_validate(row.model_dump()))
            for row in ascending
        ],
        chunk_size=10,
    )

    port = MockSearchAdapter(
        state=state,
        spec=SearchSpec(
            name="rows",
            model_type=_Row,
            fields=searchable_fields(),
            default_sort={"title": "desc"},
            snapshot=rs_spec,
        ),
        result_snapshot=SearchResultSnapshot(store=store),
    )
    page = await port.search_page(
        "python", None, {"limit": 10}, snapshot={"id": "run-1", "fingerprint": stale}
    )

    assert [hit.title for hit in page.hits] == [row.title for row in reversed(ascending)]


async def test_a_lenient_field_is_no_sort_key(harness: SearchHarness) -> None:
    """A lenient field has no stored value, so the real backends refuse to sort by it."""

    state = MockState()
    spec = SearchSpec(
        name="rows",
        model_type=_Row,
        fields=searchable_fields(),
        lenient_read_fields=frozenset({"category"}),
    )

    with pytest.raises(CoreException) as refused:
        await MockSearchAdapter(state=state, spec=spec).search(
            "", None, {"limit": 5}, {"category": "asc"}
        )

    assert refused.value.code == "field_not_on_read_model"


async def test_an_invalid_sort_is_refused_before_a_snapshot_is_read() -> None:
    """A snapshot keyed on what the sort resolves to must not let an unknown key past."""

    state = MockState()
    rs_spec = SearchResultSnapshotSpec(name="snap", enabled=True)
    store = MockSearchResultSnapshotAdapter(state=state, spec=rs_spec)
    spec = SearchSpec(name="rows", model_type=_Row, fields=searchable_fields(), snapshot=rs_spec)
    sorts = {"id": "asc", "nope": "asc"}
    # Keyed as the unchecked sort resolves: ended at the id, the unknown key dropped.
    key = SearchResultSnapshot.simple_search_fingerprint(
        "python", None, {"id": "asc"}, spec_name="rows", variant="offset"
    )
    await store.put_run(run_id="run-1", fingerprint=key, ordered_ids=[], chunk_size=10)
    port = MockSearchAdapter(
        state=state, spec=spec, result_snapshot=SearchResultSnapshot(store=store)
    )

    with pytest.raises(CoreException) as refused:
        await port.search_page(
            "python", None, {"limit": 5}, sorts, snapshot={"id": "run-1", "fingerprint": key}
        )

    assert refused.value.code == "field_not_on_read_model"
