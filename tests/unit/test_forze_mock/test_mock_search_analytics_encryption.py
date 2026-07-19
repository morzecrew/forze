"""Mock search & analytics field encryption — declared fields seal at rest, reads decrypt.

# covers: forze_mock.execution.factories

The mock search/analytics factories run the same shared, fail-closed codec resolution
their real counterparts (Meilisearch, ClickHouse/BigQuery/Postgres) do, so an upserted
search document and an ingested analytics row hold envelopes in ``MockState`` while
every read path returns plaintext — with no pre-pass anywhere (the module's keyring
fills synchronously).
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.search import SearchSpec
from forze.application.execution import ExecutionContext
from forze.base.crypto import ENVELOPE_B64_PREFIX
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
