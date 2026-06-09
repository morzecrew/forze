"""Unit tests for Meilisearch native federation merge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    SearchResultSnapshotMeta,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import CoreException
from forze_meilisearch.adapters.search.federated import (
    MeilisearchFederatedSearchAdapter,
    _hit_index_uid,
)

# ----------------------- #


class _Hit(BaseModel):
    id: str
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


def _leg(name: str, index_uid: str) -> MagicMock:
    leg = MagicMock()
    leg.index_uid = index_uid
    leg._resolved_index_uid = AsyncMock(return_value=index_uid)
    leg.spec = _mem(name)
    leg.field_map = {}
    leg.build_filter = MagicMock(return_value=None)
    leg.from_hit = lambda raw: dict(raw)
    return leg


@pytest.mark.asyncio
async def test_federation_skips_zero_weight_legs() -> None:
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_a"}}],
            estimated_total_hits=1,
        )
    )

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page(
        "q",
        options={"member_weights": {"a": 1.0, "b": 0.0}},
    )

    assert page.count == 1
    assert page.hits[0].member == "a"
    queries = client.multi_search.await_args.args[0]
    assert len(queries) == 1
    assert queries[0].index_uid == "idx_a"
    assert queries[0].federation_options is not None
    assert queries[0].federation_options.weight == 1.0


def test_hit_index_uid_reads_federation_metadata() -> None:
    assert _hit_index_uid({"_federation": {"indexUid": "idx_a"}}) == "idx_a"
    assert _hit_index_uid({"_federation": {"index_uid": "idx_b"}}) == "idx_b"
    assert _hit_index_uid({"id": "1"}) is None


@pytest.mark.asyncio
async def test_federation_all_zero_weights_returns_empty() -> None:
    client = MagicMock()
    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_empty",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page(
        "q",
        options={"member_weights": {"a": 0.0, "b": 0.0}},
        pagination={"offset": 0, "limit": 10},
    )

    assert page.count == 0
    client.multi_search.assert_not_called()


@pytest.mark.asyncio
async def test_federation_resolves_member_via_leg_index_fallback() -> None:
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_b"}}],
            estimated_total_hits=1,
        )
    )

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_resolve",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page("q", options={"member_weights": {"a": 1.0, "b": 1.0}})

    assert page.hits[0].member == "b"


@pytest.mark.asyncio
async def test_federation_member_index_map_miss_uses_resolve_loop() -> None:
    # The index->member map omits idx_b (first resolve returns a stale uid), so the
    # per-hit fallback loop must re-resolve each leg to recover the member name.
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_b"}}],
            estimated_total_hits=1,
        )
    )

    leg_a = _leg("a", "idx_a")
    leg_b = _leg("b", "idx_b")
    # First call (building the map) yields a non-matching uid; later calls match.
    leg_b._resolved_index_uid = AsyncMock(side_effect=["stale_uid", "idx_b", "idx_b"])

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_map_miss",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", leg_a), ("b", leg_b)),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page("q")

    assert page.hits[0].member == "b"


@pytest.mark.asyncio
async def test_federation_unresolvable_hit_index_raises() -> None:
    # A hit whose index uid matches no leg falls through the resolve loop with no member.
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_unknown"}}],
            estimated_total_hits=1,
        )
    )

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_unresolvable",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )

    with pytest.raises(RuntimeError):
        await adapter.search_page("q")


# ----------------------- #
# Adapter construction validation (__attrs_post_init__)


def test_legs_length_must_match_members() -> None:
    with pytest.raises(CoreException):
        MeilisearchFederatedSearchAdapter(
            federated_spec=FederatedSearchSpec(
                name="fed",
                members=(_mem("a"), _mem("b")),
            ),
            legs=(("a", _leg("a", "idx_a")),),
            client=MagicMock(),
        )


def test_leg_member_name_must_match_spec() -> None:
    with pytest.raises(CoreException):
        MeilisearchFederatedSearchAdapter(
            federated_spec=FederatedSearchSpec(
                name="fed",
                members=(_mem("a"), _mem("b")),
            ),
            legs=(("a", _leg("a", "idx_a")), ("WRONG", _leg("b", "idx_b"))),
            client=MagicMock(),
        )


# ----------------------- #
# Routing edge cases (projection unsupported, cursor unsupported)


def _basic_adapter(client: MagicMock | None = None) -> MeilisearchFederatedSearchAdapter:
    return MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client or MagicMock(),
        merge="federation",
    )


@pytest.mark.asyncio
async def test_projection_is_not_supported() -> None:
    adapter = _basic_adapter()

    with pytest.raises(CoreException):
        await adapter.project_search(["label"], "q")


@pytest.mark.asyncio
async def test_cursor_search_is_not_supported() -> None:
    adapter = _basic_adapter()

    with pytest.raises(CoreException):
        await adapter.search_cursor("q")


# ----------------------- #
# Federation builds filter and sort params on the leg queries


@pytest.mark.asyncio
async def test_federation_passes_filter_and_sort_to_queries() -> None:
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_a"}}],
            estimated_total_hits=1,
        )
    )

    leg_a = _leg("a", "idx_a")
    leg_a.build_filter = MagicMock(return_value="label = 'x'")
    leg_b = _leg("b", "idx_b")
    leg_b.build_filter = MagicMock(return_value="label = 'x'")

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_sorted",
            members=(_mem("a"), _mem("b")),
        ),
        legs=(("a", leg_a), ("b", leg_b)),
        client=client,
        merge="federation",
    )

    page = await adapter.search_page("q", sorts={"label": "asc"})

    assert page.count == 1
    queries = client.multi_search.await_args.args[0]
    assert all(q.filter == "label = 'x'" for q in queries)
    assert all(q.sort is not None for q in queries)


@pytest.mark.asyncio
async def test_federation_omits_attributes_when_no_active_fields() -> None:
    # Zero field weights => attributes_to_search_on returns None => the param is omitted.
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_a"}}],
            estimated_total_hits=1,
        )
    )

    zero_mem_a = SearchSpec(
        name="a", model_type=_Hit, fields=["label"], default_weights={"label": 0.0}
    )
    zero_mem_b = SearchSpec(
        name="b", model_type=_Hit, fields=["label"], default_weights={"label": 0.0}
    )
    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=FederatedSearchSpec(
            name="fed_no_attrs",
            members=(zero_mem_a, zero_mem_b),
        ),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
    )
    await adapter.search_page("q")

    queries = client.multi_search.await_args.args[0]
    assert all(q.attributes_to_search_on is None for q in queries)


@pytest.mark.asyncio
async def test_federation_applies_limit_in_federation_payload() -> None:
    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_a"}}],
            estimated_total_hits=1,
        )
    )

    adapter = _basic_adapter(client)
    await adapter.search_page("q", pagination={"offset": 2, "limit": 7})

    federation = client.multi_search.await_args.kwargs["federation"]
    assert federation == {"offset": 2, "limit": 7}


@pytest.mark.asyncio
async def test_federation_empty_queries_countless_returns_empty() -> None:
    # search() (no count) with all-zero weights exercises the total=None empty path.
    adapter = _basic_adapter()
    page = await adapter.search(
        "q",
        options={"member_weights": {"a": 0.0, "b": 0.0}},
    )

    assert list(page.hits) == []


# ----------------------- #
# Snapshot read short-circuit and write paths (pure in-memory store stub)


def _snapshot_spec() -> SearchResultSnapshotSpec:
    return SearchResultSnapshotSpec(name="snap", enabled=True, max_ids=100)


def _fed_spec_with_snapshot(name: str) -> FederatedSearchSpec[_Hit]:
    return FederatedSearchSpec(
        name=name,
        members=(_mem("a"), _mem("b")),
        snapshot=_snapshot_spec(),
    )


@pytest.mark.asyncio
async def test_federation_snapshot_read_short_circuits() -> None:
    # When the snapshot store returns a stored page, no client search happens.
    member = "a"
    hit = _Hit(id="42", label="stored")
    key = SearchResultSnapshot.federated_record_key_string(member, hit)

    store = MagicMock()
    store.get_id_range = AsyncMock(return_value=[key])
    store.get_meta = AsyncMock(
        return_value=SearchResultSnapshotMeta(
            run_id="run-1",
            fingerprint="fp",
            total=1,
            chunk_size=10,
            complete=True,
        )
    )
    result_snapshot = SearchResultSnapshot(store=store)

    client = MagicMock()
    client.multi_search = AsyncMock()

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=_fed_spec_with_snapshot("fed_snap_read"),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
        result_snapshot=result_snapshot,
    )

    page = await adapter.search_page("q", snapshot={"id": "run-1"})

    assert page.count == 1
    assert page.hits[0].member == "a"
    assert page.hits[0].hit.id == "42"
    client.multi_search.assert_not_called()


@pytest.mark.asyncio
async def test_federation_snapshot_read_miss_then_searches_and_writes() -> None:
    # get_id_range -> None means a miss, so the adapter searches and writes a snapshot.
    store = MagicMock()
    store.get_id_range = AsyncMock(return_value=None)
    store.get_meta = AsyncMock(return_value=None)
    store.put_run = AsyncMock(return_value=None)
    result_snapshot = SearchResultSnapshot(store=store)

    client = MagicMock()
    client.multi_search = AsyncMock(
        return_value=MagicMock(
            hits=[{"id": "1", "label": "x", "_federation": {"indexUid": "idx_a"}}],
            estimated_total_hits=1,
        )
    )

    adapter = MeilisearchFederatedSearchAdapter(
        federated_spec=_fed_spec_with_snapshot("fed_snap_write"),
        legs=(("a", _leg("a", "idx_a")), ("b", _leg("b", "idx_b"))),
        client=client,
        merge="federation",
        result_snapshot=result_snapshot,
    )

    page = await adapter.search_page("q", snapshot={"id": "missing", "mode": True})

    client.multi_search.assert_awaited_once()
    store.put_run.assert_awaited_once()
    assert page.snapshot is not None
    assert page.snapshot.total == 1
