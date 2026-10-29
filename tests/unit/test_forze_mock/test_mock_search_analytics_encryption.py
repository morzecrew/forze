"""Mock search & analytics field encryption — declared fields seal at rest, reads decrypt.

# covers: forze_mock.execution.factories

The mock search/analytics factories run the same shared, fail-closed codec resolution
their real counterparts (Meilisearch, ClickHouse/BigQuery/Postgres) do, so an upserted
search document and an ingested analytics row hold envelopes in ``MockState`` while
every read path returns plaintext — with no pre-pass anywhere (the module's keyring
fills synchronously).
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.crypto import ENVELOPE_B64_PREFIX
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #


def _sealed(value: object) -> bool:
    return isinstance(value, str) and value.startswith(ENVELOPE_B64_PREFIX)


@pytest.fixture
def state() -> MockState:
    return MockState()


@pytest.fixture
def ctx(state: MockState) -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=state)())


# ....................... #
# search


class _Article(BaseModel):
    id: str
    title: str
    body: str = ""


def _search_spec() -> SearchSpec[_Article]:
    return SearchSpec(
        name="articles",
        model_type=_Article,
        fields=["title"],
        encryption=FieldEncryption(encrypted=frozenset({"body"})),
    )


@pytest.mark.asyncio
async def test_search_upsert_seals_and_results_decrypt(
    ctx: ExecutionContext, state: MockState
) -> None:
    spec = _search_spec()
    doc_id = str(uuid4())

    await ctx.search.command(spec).upsert(
        [_Article(id=doc_id, title="alpha", body="confidential")]
    )

    stored = next(iter(state.documents["articles"].values()))
    assert _sealed(stored["body"]), stored["body"]
    assert stored["title"] == "alpha"  # the indexed text field stays matchable

    page = await ctx.search.query(spec).search("alpha", pagination={"limit": 10})
    assert [hit.id for hit in page.hits] == [doc_id]
    assert page.hits[0].body == "confidential"


@pytest.mark.asyncio
async def test_search_text_match_cannot_see_sealed_content(
    ctx: ExecutionContext, state: MockState
) -> None:
    """The divergence this closes: the mock used to store plaintext, so a text query
    could match content that Meilisearch — indexing ciphertext — can never match."""

    spec = _search_spec()
    await ctx.search.command(spec).upsert(
        [_Article(id=str(uuid4()), title="alpha", body="confidential")]
    )

    page = await ctx.search.query(spec).search("confidential", pagination={"limit": 10})
    assert page.hits == []


# ....................... #
# analytics


class _Row(BaseModel):
    event: str
    note: str | None = None


class _Params(BaseModel):
    day: str = "2026-01-01"


def _analytics_spec() -> AnalyticsSpec[_Row, _Row]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"recent": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Row,
        encryption=FieldEncryption(encrypted=frozenset({"note"})),
    )


@pytest.mark.asyncio
async def test_analytics_ingest_seals_and_reads_decrypt(
    ctx: ExecutionContext, state: MockState
) -> None:
    spec = _analytics_spec()

    result = await ctx.analytics.ingest(spec).append([_Row(event="signup", note="pii")])
    assert result is not None and result.accepted == 1

    logged = state.analytics_ingest_log["events"][0]
    assert _sealed(logged["note"]), logged["note"]
    assert logged["event"] == "signup"  # undeclared columns stay analyzable

    # Round-trip: the sealed row programmed as a query hit decodes to plaintext,
    # exactly as a real warehouse read decrypts what its ingest sealed.
    state.analytics_query_hits["events"] = {"recent": [dict(logged)]}
    page = await ctx.analytics.query(spec).run_page("recent", _Params())
    assert page.hits[0].note == "pii"


# ....................... #
# in-place search over a document's rows (sealed_by)


class _Note(Document):
    title: str
    body: str


class _NoteCreate(CreateDocumentCmd):
    title: str
    body: str


class _NoteUpdate(BaseDTO):
    body: str | None = None


class _NoteRead(ReadDocument):
    title: str
    body: str


_NOTE_POLICY = FieldEncryption(encrypted=frozenset({"body"}))


async def _write_note(ctx: ExecutionContext, name: str) -> None:
    spec = DocumentSpec(
        name=name,
        read=_NoteRead,
        write={"domain": _Note, "create_cmd": _NoteCreate, "update_cmd": _NoteUpdate},  # type: ignore[arg-type]
        encryption=_NOTE_POLICY,
    )
    await ctx.document.command(spec).create(_NoteCreate(title="ledger", body=f"{name}-pii"))


def _note_search(name: str, sealed_by: str | None) -> SearchSpec[_NoteRead]:
    return SearchSpec(
        name=name,
        model_type=_NoteRead,
        fields=["title"],
        encryption=_NOTE_POLICY,
        sealed_by=sealed_by,
    )


@pytest.mark.asyncio
async def test_search_reads_a_documents_rows_only_when_sealed_by_names_it(
    ctx: ExecutionContext,
) -> None:
    await _write_note(ctx, "notes")

    page = await ctx.search.query(_note_search("notes", "notes")).search("ledger")
    assert [hit.body for hit in page.hits] == ["notes-pii"]

    with pytest.raises(CoreException) as refused:
        await ctx.search.query(_note_search("notes", None)).search("ledger")

    assert refused.value.code == "core.crypto.aead_auth_failed"


@pytest.mark.asyncio
async def test_federated_members_each_open_their_own_documents_rows(
    ctx: ExecutionContext,
) -> None:
    await _write_note(ctx, "alpha")
    await _write_note(ctx, "beta")
    fed = FederatedSearchSpec(
        name="fed", members=[_note_search("alpha", "alpha"), _note_search("beta", "beta")]
    )

    page = await ctx.deps.provide(FederatedSearchQueryDepKey)(ctx, fed).search("ledger")

    assert sorted((h.member, h.hit.body) for h in page.hits) == [
        ("alpha", "alpha-pii"),
        ("beta", "beta-pii"),
    ]


@pytest.mark.asyncio
async def test_mock_hub_decodes_its_rows_under_the_hubs_sealed_by(
    ctx: ExecutionContext,
) -> None:
    """The hub's sealed_by governs the rows it decodes; a member may leave its own unset."""

    await _write_note(ctx, "alpha")
    hub = HubSearchSpec(
        name="hub",
        model_type=_NoteRead,
        members=[_note_search("alpha", None)],
        encryption=_NOTE_POLICY,
        sealed_by="alpha",
    )

    page = await ctx.deps.provide(HubSearchQueryDepKey)(ctx, hub).search("ledger")
    assert [h.body for h in page.hits] == ["alpha-pii"]


@pytest.mark.parametrize("key", [SearchCommandDepKey, SearchManagementDepKey])
def test_mock_index_write_path_refuses_sealed_by(ctx: ExecutionContext, key: Any) -> None:
    with pytest.raises(CoreException) as refused:
        ctx.deps.provide(key)(ctx, _note_search("notes", "notes"))

    assert refused.value.code == "core.search.encryption_wiring"
