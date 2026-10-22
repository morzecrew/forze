"""Unit tests for :class:`PostgresVectorSearchAdapterV2` (no database)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException

pytest.importorskip("psycopg")

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search import PostgresVectorSearchAdapter
from forze_postgres.adapters.search._vector_sql import vector_param_literal


class _M(BaseModel):
    id: UUID
    label: str


def _minimal_vector_port(
    *,
    join_pairs: tuple[tuple[str, str], ...] | None = None,
) -> PostgresVectorSearchAdapter[_M]:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(return_value={})
    spec = SearchSpec(name="t", model_type=_M, fields=["id", "label"])
    return PostgresVectorSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        index_relation=("public", "idx"),
        relation=("public", "heap"),
        index_heap_relation=("public", "heap"),
        embedder=MagicMock(),
        embeddings_spec=EmbeddingsSpec(name="e", dimensions=3),
        vector_column="emb",
        client=MagicMock(),
        model_type=_M,
        introspector=intro,
        tenant_aware=False,
        tenant_provider=None,
        filter_table_alias="v",
        join_pairs=join_pairs,
    )


def test_join_pairs_must_use_unique_projection_columns() -> None:
    with pytest.raises(CoreException, match="unique projection"):
        _minimal_vector_port(
            join_pairs=(("id", "a"), ("id", "b")),
        )


@pytest.mark.asyncio
async def test_search_with_cursor_browse_calls_fetch_all() -> None:
    uid = UUID("00000000-0000-4000-8000-000000000001")
    port = _minimal_vector_port()
    port.client.fetch_all = AsyncMock(
        return_value=[{"id": uid, "label": "only"}],
    )
    page = await port.search_cursor("", cursor={"limit": 5})
    assert len(page.hits) == 1
    assert page.hits[0].id == uid
    port.client.fetch_all.assert_awaited_once()


@pytest.mark.asyncio
async def test_search_with_cursor_ranked_calls_embed_and_fetch() -> None:
    uid = UUID("00000000-0000-4000-8000-000000000002")
    port = _minimal_vector_port()
    qvec = [0.1, 0.2, 0.3]
    port.embedder.embed_one = AsyncMock(return_value=qvec)
    port.client.fetch_all = AsyncMock(
        return_value=[
            {
                "id": uid,
                "label": "hit",
                "_vector_rank": 0.42,
            },
        ],
    )
    page = await port.project_search_cursor(
        ["id", "label"],
        "q",
        cursor={"limit": 3},
    )
    assert len(page.hits) == 1
    assert set(page.hits[0].keys()) == {"id", "label"}
    port.embedder.embed_one.assert_awaited_once()
    port.client.fetch_all.assert_awaited_once()
    call = port.client.fetch_all.call_args
    params = call[0][1]
    assert vector_param_literal(qvec) in params


@pytest.mark.asyncio
async def test_a_query_vector_ranks_a_blank_query_without_the_embedder() -> None:
    port = _minimal_vector_port()
    port.embedder.embed_one = AsyncMock()
    port.client.fetch_all = AsyncMock(return_value=[])
    qvec = [0.5, 0.25, 0.125]

    await port.search_cursor("", cursor={"limit": 3}, options={"query_vector": qvec})
    await port.search("", options={"query_vector": qvec})

    port.embedder.embed_one.assert_not_awaited()
    assert port.client.fetch_all.await_count == 2

    for call in port.client.fetch_all.call_args_list:
        assert vector_param_literal(qvec) in call[0][1]


@pytest.mark.asyncio
async def test_an_explicit_none_query_vector_browses_a_blank_query() -> None:
    port = _minimal_vector_port()
    port.embedder.embed_one = AsyncMock()
    port.client.fetch_all = AsyncMock(return_value=[])

    page = await port.search("", options={"query_vector": None})  # type: ignore[typeddict-item]

    port.embedder.embed_one.assert_not_awaited()
    assert page.scores is None


@pytest.mark.asyncio
@pytest.mark.parametrize("qvec", [[0.1, 0.2], [0.1, 0.2, float("nan")], "abc"])
async def test_a_query_vector_of_the_wrong_shape_is_refused(qvec: object) -> None:
    port = _minimal_vector_port()

    with pytest.raises(CoreException) as caught:
        await port.search("", options={"query_vector": qvec})  # type: ignore[typeddict-item]

    assert caught.value.kind.value == "validation"


@pytest.mark.asyncio
async def test_a_query_vector_alongside_a_query_is_refused() -> None:
    port = _minimal_vector_port()

    with pytest.raises(CoreException) as caught:
        await port.search("text", options={"query_vector": [0.1, 0.2, 0.3]})

    assert caught.value.code == "search_query_vector_with_query"


def test_the_snapshot_fingerprint_tells_query_vectors_apart() -> None:
    port = _minimal_vector_port()

    one = port._fingerprint_extras({"query_vector": [0.1, 0.2, 0.3]})
    other = port._fingerprint_extras({"query_vector": [0.3, 0.2, 0.1]})

    assert one != other
    assert "query_vector" not in (port._fingerprint_extras(None) or {})


@pytest.mark.asyncio
async def test_a_search_without_snapshots_formats_the_query_vector_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SQL parameter is the one formatting a search needs; nothing else re-formats it."""

    from forze_postgres.adapters.search import _leg_vector

    calls: list[int] = []

    def counting(values: list[float]) -> str:
        calls.append(len(values))
        return vector_param_literal(values)

    monkeypatch.setattr(_leg_vector, "vector_param_literal", counting)

    port = _minimal_vector_port()
    port.client.fetch_all = AsyncMock(return_value=[])

    await port.search("", options={"query_vector": [0.5, 0.25, 0.125]})

    assert calls == [3]


def test_the_snapshot_fingerprint_digests_the_query_vector() -> None:
    port = _minimal_vector_port()

    extras = port._fingerprint_extras({"query_vector": [0.5, 0.25, 0.125]}) or {}
    same = port._fingerprint_extras({"query_vector": (0.5, 0.25, 0.125)}) or {}

    other = port._fingerprint_extras({"query_vector": [0.5, 0.25, 0.126]}) or {}

    assert extras["query_vector"] == same["query_vector"]
    assert extras["query_vector"] != other["query_vector"]
    # A fixed-size digest, not the vector's text: a 1536-wide vector must not cost a 20 KB key.
    assert len(str(extras["query_vector"])) == 32
    assert vector_param_literal([0.5, 0.25, 0.125]) not in str(extras["query_vector"])
