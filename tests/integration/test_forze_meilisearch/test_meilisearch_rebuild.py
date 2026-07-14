"""`rebuild_search_index` drives a **real** Meilisearch index from a document plane.

The mock search adapter writes into the same in-memory bucket the mock document store uses,
so a mock-only test cannot see what a rebuild actually asks a search engine to do: batched
``add_documents`` tasks, an async task queue the adapter has to await, and a delete that is
rendered as a filter rather than an id list on a tenant-tagged index. This wires the real
index behind the sweep (with the mock document plane in front of it, which is the half the
sweep only *reads*) and asserts the end state by **searching** — the observable an operator
actually has — rather than by inspecting the writer's own bookkeeping.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.search import (
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_kits.integrations.search import SearchRebuildReport, rebuild_search_index
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.factories import (
    ConfigurableMeilisearchSearchManagement,
)
from forze_mock import MockState, MockStateDepKey
from forze_mock.execution.factories import ConfigurableMockDocument
from forze_mock.execution.module import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


class Gadget(DocWithSoftDeletion):
    name: str = ""


class GadgetCreate(CreateDocumentCmd):
    name: str


class GadgetUpdate(UpdateCmdWithSoftDeletion):
    name: str | None = None


class GadgetRead(ReadDocument):
    name: str
    is_deleted: bool = False


GADGET_SPEC = DocumentSpec(
    name="gadgets",
    read=GadgetRead,
    write=DocumentWriteTypes(domain=Gadget, create_cmd=GadgetCreate, update_cmd=GadgetUpdate),
)
GADGET_INDEX = SearchSpec(
    name="gadgets",
    model_type=GadgetRead,
    fields=["name"],
    facetable_fields={"is_deleted"},
)

_INDEX_UID = "rebuild_it"


# ....................... #


def _deps(meilisearch_client) -> Deps:
    """Mock document plane (the sweep only reads it) + the REAL Meilisearch index."""

    mock_module = MockDepsModule(state=MockState())
    document = ConfigurableMockDocument(module=mock_module)
    config = MeilisearchSearchConfig(index_uid=_INDEX_UID, wait_for_tasks=True)

    return Deps.plain(
        {
            MockStateDepKey: mock_module.state,
            DocumentQueryDepKey: document,
            DocumentCommandDepKey: document,
            MeilisearchClientDepKey: meilisearch_client,
            SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(config=config),
            SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(config=config),
            SearchQueryDepKey: ConfigurableMeilisearchSearch(config=config),
        }
    )


async def _names_in_index(ctx) -> set[str]:
    page = await ctx.search.query(GADGET_INDEX).search("", pagination={"offset": 0, "limit": 100})
    return {row.name for row in page.hits}


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rebuild_fills_a_real_index_and_never_indexes_a_soft_deleted_row(
    meilisearch_client,
) -> None:
    ctx = context_from_deps(_deps(meilisearch_client))
    docs = ctx.doc.command(GADGET_SPEC)

    mgmt = ctx.search.management(GADGET_INDEX)
    await mgmt.ensure_index()
    await mgmt.delete_all()

    # Three rows written straight to the document plane — no sync is bound, so nothing
    # has ever carried them into the index. This is the backfill case.
    live_a = await docs.create(GadgetCreate(name="alpha"))
    live_b = await docs.create(GadgetCreate(name="beta"))
    soft = await docs.create(GadgetCreate(name="gamma"))
    await docs.update(soft.id, soft.rev, GadgetUpdate(is_deleted=True))

    assert await _names_in_index(ctx) == set()

    report = await rebuild_search_index(
        ctx.doc.query(GADGET_SPEC),
        ctx.search.command(GADGET_INDEX),
        document=GADGET_SPEC,
        search=GADGET_INDEX,
        chunk_size=2,  # forces more than one batch — the sweep's paging is exercised
    )

    # Searching is the observable: the two live rows are findable, and the soft-deleted
    # one is not a hit that GET would then 404.
    assert await _names_in_index(ctx) == {"alpha", "beta"}
    assert report == SearchRebuildReport(indexed=2, removed=1)
    assert {live_a.name, live_b.name} == {"alpha", "beta"}

    await mgmt.delete_all()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rebuild_evicts_a_row_the_index_still_holds_after_a_soft_delete(
    meilisearch_client,
) -> None:
    # The drifted-index repair: the row reached the index while it was live, its
    # soft-delete never did. A sweep that only upserted would leave the ghost forever.
    ctx = context_from_deps(_deps(meilisearch_client))
    docs = ctx.doc.command(GADGET_SPEC)

    mgmt = ctx.search.management(GADGET_INDEX)
    await mgmt.ensure_index()
    await mgmt.delete_all()

    row = await docs.create(GadgetCreate(name="delta"))
    await ctx.search.command(GADGET_INDEX).upsert_many(
        [await ctx.doc.query(GADGET_SPEC).get(pk=row.id)]
    )
    assert await _names_in_index(ctx) == {"delta"}

    await docs.update(row.id, row.rev, GadgetUpdate(is_deleted=True))

    report = await rebuild_search_index(
        ctx.doc.query(GADGET_SPEC),
        ctx.search.command(GADGET_INDEX),
        document=GADGET_SPEC,
        search=GADGET_INDEX,
    )

    assert await _names_in_index(ctx) == set()
    assert report == SearchRebuildReport(indexed=0, removed=1)

    await mgmt.delete_all()
