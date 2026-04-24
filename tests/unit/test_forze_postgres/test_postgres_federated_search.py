"""Federated search: RRF merge and PostgresFederatedSearchAdapter behavior."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, page_from_limit_offset
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    HubSearchSpec,
    SearchResultSnapshotMeta,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.base.errors import CoreError
from forze_postgres.adapters.search.federated import (
    PostgresFederatedSearchAdapter,
    weighted_rrf_merge_rows,
)
from forze_postgres.adapters.search.federated_snapshot import (
    federated_fingerprint,
    federated_row_key_string,
)
from forze_postgres.execution.deps.configs import validate_postgres_federated_search_conf
from forze_postgres.execution.deps.deps import ConfigurablePostgresFederatedSearch

# ----------------------- #


class _Hit(BaseModel):
    id: int
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


def _fed() -> FederatedSearchSpec[_Hit]:
    return FederatedSearchSpec(
        name="fed",
        members=(_mem("a"), _mem("b")),
    )


def _fed_with_result_snapshot() -> FederatedSearchSpec[_Hit]:
    return FederatedSearchSpec(
        name="fed",
        members=(_mem("a"), _mem("b")),
        result_snapshot=SearchResultSnapshotSpec(
            name="snap",
            enabled=True,
            ttl=timedelta(minutes=5),
        ),
    )


def test_weighted_rrf_merge_applies_branch_weights() -> None:
    x = _Hit(id=1, label="x")
    y = _Hit(id=2, label="y")
    k = 60
    merged = weighted_rrf_merge_rows(
        leg_rows=(
            ("a", (x, y), 2.0),
            ("b", (y, x), 1.0),
        ),
        k=k,
    )
    scores = {m.hit.id: sc for m, sc in merged}
    # y ranks 2 on a (2/(60+2)) and 1 on b (1/(60+1)); x is the reverse.
    assert scores[2] > scores[1]


def test_weighted_rrf_skips_non_positive_weight_leg() -> None:
    only = weighted_rrf_merge_rows(
        leg_rows=(
            ("a", (_Hit(id=1),), 0.0),
            ("b", (_Hit(id=2),), 1.0),
        ),
        k=60,
    )
    assert len(only) == 1
    assert only[0][0].hit.id == 2
    assert only[0][0].member == "b"


def test_validate_postgres_federated_search_conf_requires_two_members() -> None:
    with pytest.raises(CoreError, match="at least two"):
        validate_postgres_federated_search_conf(
            {
                "members": {
                    "a": {
                        "index": ("public", "i"),
                        "read": ("public", "r"),
                        "engine": "pgroonga",
                    },
                },
            },
        )


def test_validate_postgres_federated_search_conf_accepts_embedded_hub_member() -> None:
    validate_postgres_federated_search_conf(
        {
            "members": {
                "hub": {
                    "hub": ("public", "hub_v"),
                    "members": {
                        "in_a": {
                            "index": ("public", "i_a"),
                            "read": ("public", "r_a"),
                            "engine": "pgroonga",
                            "hub_fk": "a_id",
                        },
                        "in_b": {
                            "index": ("public", "i_b"),
                            "read": ("public", "r_b"),
                            "engine": "pgroonga",
                            "hub_fk": "b_id",
                        },
                    },
                },
                "flat": {
                    "index": ("public", "i_f"),
                    "read": ("public", "r_f"),
                    "engine": "pgroonga",
                },
            },
        },
    )


@pytest.mark.asyncio
async def test_federated_search_reads_snapshot_without_running_legs() -> None:
    h = _Hit(id=1, label="x")
    row_key = federated_row_key_string("a", h)
    fp = federated_fingerprint(
        "q", None, None, spec_name="fed", rrf_k=60
    )
    store = MagicMock()
    store.get_id_range = AsyncMock(return_value=[row_key])
    store.get_meta = AsyncMock(
        return_value=SearchResultSnapshotMeta(
            run_id="run-1",
            fingerprint=fp,
            total=1,
            chunk_size=100,
            complete=True,
        )
    )
    pa = MagicMock()
    pa.search = AsyncMock()
    pb = MagicMock()
    pb.search = AsyncMock()
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed_with_result_snapshot(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=10,
        snapshot_store=store,
    )
    page = await adapter.search(
        "q",
        pagination={"offset": 0, "limit": 5},
        options={
            "result_snapshot": {
                "id": "run-1",
                "fingerprint": fp,
            }
        },
        return_count=True,
    )
    pa.search.assert_not_awaited()
    pb.search.assert_not_awaited()
    assert page.count == 1
    assert len(page.hits) == 1
    assert page.hits[0].member == "a"
    assert page.hits[0].hit.id == 1
    assert page.result_snapshot is not None
    assert page.result_snapshot.id == "run-1"
    assert page.result_snapshot.fingerprint == fp
    store.get_id_range.assert_awaited_once()
    get_kw = store.get_id_range.call_args[1]
    assert get_kw.get("expected_fingerprint") == fp


@pytest.mark.asyncio
async def test_federated_search_materializes_snapshot_after_merge() -> None:
    h = _Hit(id=1, label="x")

    async def one(*_a, **_kw):
        return page_from_limit_offset([h], {}, total=None)

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=one)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=one)
    store = MagicMock()
    store.put_run = AsyncMock()
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed_with_result_snapshot(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=10,
        snapshot_store=store,
    )
    page = await adapter.search(
        "q",
        pagination={"offset": 0, "limit": 5},
        return_count=True,
    )
    assert page.result_snapshot is not None
    run_id = page.result_snapshot.id
    assert run_id
    assert page.result_snapshot.capped is False
    store.put_run.assert_awaited_once()
    pr_kw = store.put_run.call_args[1]
    assert pr_kw["run_id"] == run_id
    assert pr_kw["ordered_ids"]


@pytest.mark.asyncio
async def test_federated_search_skips_zero_weight_members() -> None:
    pa = MagicMock()
    pa.search = AsyncMock(
        return_value=page_from_limit_offset([_Hit(id=1)], {}, total=None)
    )
    pb = MagicMock()
    pb.search = AsyncMock(
        return_value=page_from_limit_offset([_Hit(id=2)], {}, total=None)
    )
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=50,
    )
    page = await adapter.search(
        "q",
        options={"member_weights": {"a": 0.0, "b": 1.0}},
        return_count=True,
    )
    pa.search.assert_not_awaited()
    pb.search.assert_awaited_once()
    assert page.count == 1
    assert len(page.hits) == 1
    assert isinstance(page.hits[0], FederatedSearchReadModel)
    assert page.hits[0].member == "b"
    assert page.hits[0].hit.id == 2


@pytest.mark.asyncio
async def test_federated_search_pagination_on_merged_pool() -> None:
    async def leg_a(*_a, **_kw):
        return page_from_limit_offset(
            [_Hit(id=i) for i in range(3)], {}, total=None
        )

    async def leg_b(*_a, **_kw):
        return page_from_limit_offset(
            [_Hit(id=i + 10) for i in range(3)], {}, total=None
        )

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=leg_a)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=leg_b)
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_k=60,
        rrf_per_leg_limit=10,
    )
    page = await adapter.search(
        "q", pagination={"offset": 2, "limit": 2}, return_count=True
    )
    assert page.count == 6
    assert len(page.hits) == 2


@pytest.mark.asyncio
async def test_federated_search_all_members_disabled_returns_empty() -> None:
    na = MagicMock()
    na.search = AsyncMock()
    nb = MagicMock()
    nb.search = AsyncMock()
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", na), ("b", nb)),
    )
    page = await adapter.search(
        "q", options={"members": []}, return_count=True
    )
    assert page.hits == []
    assert page.count == 0


@pytest.mark.asyncio
async def test_federated_search_all_members_disabled_countless_page() -> None:
    """Disabled legs short-circuit without ``return_count`` (total stays unknown)."""
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", MagicMock()), ("b", MagicMock())),
    )
    page = await adapter.search("q", options={"members": []}, return_count=False)
    assert page.hits == []
    assert isinstance(page, CountlessPage)


@pytest.mark.asyncio
async def test_federated_search_runs_legs_via_gather_db_when_postgres_client_set() -> None:
    """When ``postgres_client`` is set, leg work is dispatched through :func:`gather_db_work`."""
    pg = MagicMock()
    pg.is_in_transaction.return_value = False
    pg.query_concurrency_limit.return_value = 4

    async def leg_a(*_a, **_kw):
        return page_from_limit_offset([_Hit(id=1)], {}, total=None)

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=leg_a)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=leg_a)

    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=10,
        postgres_client=pg,
    )
    page = await adapter.search("q")
    pa.search.assert_awaited_once()
    pb.search.assert_awaited_once()
    assert len(page.hits) >= 1


@pytest.mark.asyncio
async def test_federated_search_with_cursor_is_not_implemented() -> None:
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", MagicMock()), ("b", MagicMock())),
    )
    with pytest.raises(CoreError, match="search_with_cursor"):
        await adapter.search_with_cursor("q")


def test_federated_adapter_rejects_leg_count_mismatch() -> None:
    with pytest.raises(CoreError, match="match.*members length"):
        PostgresFederatedSearchAdapter(
            federated_spec=_fed(),
            legs=(("a", MagicMock()),),
        )


def test_federated_adapter_rejects_leg_name_mismatch() -> None:
    pa = MagicMock()
    pb = MagicMock()
    with pytest.raises(CoreError, match="does not match SearchSpec.name"):
        PostgresFederatedSearchAdapter(
            federated_spec=_fed(),
            legs=(("wrong", pa), ("b", pb)),
        )


@pytest.mark.asyncio
async def test_federated_search_rejects_return_fields() -> None:
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", MagicMock()), ("b", MagicMock())),
    )
    with pytest.raises(CoreError, match="return_fields"):
        await adapter.search("q", return_fields=("id",))


class _FedRow(BaseModel):
    hit: _Hit
    member: str


@pytest.mark.asyncio
async def test_federated_search_return_type_validates_rows() -> None:
    h = _Hit(id=1, label="a")

    async def one_hit(*_a, **_kw):
        return page_from_limit_offset([h], {}, total=None)

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=one_hit)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=one_hit)
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=10,
    )
    page = await adapter.search(
        "q", return_type=_FedRow, return_count=True
    )
    assert page.count >= 1
    assert len(page.hits) >= 1
    assert isinstance(page.hits[0], _FedRow)
    assert page.hits[0].member in {"a", "b"}
    assert page.hits[0].hit.id == 1


@pytest.mark.asyncio
async def test_federated_search_applies_sorts_on_merged_field() -> None:
    async def leg_a(*_a, **_kw):
        return page_from_limit_offset(
            [_Hit(id=1, label="b"), _Hit(id=2, label="a")], {}, total=None
        )

    async def leg_b(*_a, **_kw):
        return page_from_limit_offset([_Hit(id=3, label="c")], {}, total=None)

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=leg_a)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=leg_b)
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=10,
    )
    page = await adapter.search(
        "q",
        sorts={"label": "asc"},
        pagination={"offset": 0, "limit": 10},
        return_count=True,
    )
    assert page.count == 3
    assert len(page.hits) == 3
    assert {row.hit.id for row in page.hits} == {1, 2, 3}


def _two_member_pgroonga_config() -> dict[str, object]:
    return {
        "members": {
            "a": {
                "index": ("public", "i_a"),
                "read": ("public", "r_a"),
                "engine": "pgroonga",
            },
            "b": {
                "index": ("public", "i_b"),
                "read": ("public", "r_b"),
                "engine": "pgroonga",
            },
        },
    }


def _federated_exec_context() -> MagicMock:
    ctx = MagicMock()

    def _dep(_key: object) -> MagicMock:
        return MagicMock()

    ctx.dep = _dep
    return ctx


def test_configurable_federated_search_resolves_members() -> None:
    factory = ConfigurablePostgresFederatedSearch(config=_two_member_pgroonga_config())
    adapter = factory(_federated_exec_context(), _fed())
    assert isinstance(adapter, PostgresFederatedSearchAdapter)


def test_configurable_federated_search_member_missing_in_config() -> None:
    factory = ConfigurablePostgresFederatedSearch(
        config={
            "members": {
                "x": {
                    "index": ("public", "i_x"),
                    "read": ("public", "r_x"),
                    "engine": "pgroonga",
                },
                "y": {
                    "index": ("public", "i_y"),
                    "read": ("public", "r_y"),
                    "engine": "pgroonga",
                },
            },
        },
    )
    with pytest.raises(CoreError, match="Member 'a' not found"):
        factory(_federated_exec_context(), _fed())


def test_configurable_federated_search_rejects_unknown_engine() -> None:
    cfg = _two_member_pgroonga_config()
    members = dict(cfg["members"])
    members["a"] = {**members["a"], "engine": "unknown"}
    factory = ConfigurablePostgresFederatedSearch(config={"members": members})
    with pytest.raises(CoreError, match="not supported"):
        factory(_federated_exec_context(), _fed())


def test_configurable_federated_search_fts_requires_groups() -> None:
    cfg = _two_member_pgroonga_config()
    members = {k: {**v, "engine": "fts"} for k, v in dict(cfg["members"]).items()}
    factory = ConfigurablePostgresFederatedSearch(config={"members": members})
    with pytest.raises(CoreError, match="fts_groups"):
        factory(_federated_exec_context(), _fed())


def _fed_hub_and_flat() -> FederatedSearchSpec[_Hit]:
    return FederatedSearchSpec(
        name="fed",
        members=(
            HubSearchSpec(
                name="hub",
                model_type=_Hit,
                members=(
                    SearchSpec(name="in_a", model_type=_Hit, fields=["label"]),
                    SearchSpec(name="in_b", model_type=_Hit, fields=["label"]),
                ),
            ),
            SearchSpec(name="flat", model_type=_Hit, fields=["label"]),
        ),
    )


def _federated_config_hub_and_flat() -> dict[str, object]:
    return {
        "members": {
            "hub": {
                "hub": ("public", "hub_v"),
                "members": {
                    "in_a": {
                        "index": ("public", "i_a"),
                        "read": ("public", "r_a"),
                        "engine": "pgroonga",
                        "hub_fk": "a_id",
                    },
                    "in_b": {
                        "index": ("public", "i_b"),
                        "read": ("public", "r_b"),
                        "engine": "pgroonga",
                        "hub_fk": "b_id",
                    },
                },
            },
            "flat": {
                "index": ("public", "i_f"),
                "read": ("public", "r_f"),
                "engine": "pgroonga",
            },
        },
    }


def test_configurable_federated_search_resolves_hub_member() -> None:
    factory = ConfigurablePostgresFederatedSearch(config=_federated_config_hub_and_flat())
    adapter = factory(_federated_exec_context(), _fed_hub_and_flat())
    assert isinstance(adapter, PostgresFederatedSearchAdapter)


def test_configurable_federated_search_hub_member_requires_embedded_hub_config() -> None:
    cfg = _two_member_pgroonga_config()
    members = dict(cfg["members"])
    factory = ConfigurablePostgresFederatedSearch(
        config={
            "members": {
                "hub": members["a"],
                "flat": members["b"],
            },
        },
    )
    with pytest.raises(CoreError, match="'hub' and 'members'"):
        factory(_federated_exec_context(), _fed_hub_and_flat())


def test_configurable_federated_search_searchspec_rejects_hub_shaped_config() -> None:
    cfg = {
        "members": {
            "a": {
                "hub": ("public", "hub_v"),
                "members": {
                    "in_a": {
                        "index": ("public", "i_a"),
                        "read": ("public", "r_a"),
                        "engine": "pgroonga",
                        "hub_fk": "x",
                    },
                    "in_b": {
                        "index": ("public", "i_b"),
                        "read": ("public", "r_b"),
                        "engine": "pgroonga",
                        "hub_fk": "y",
                    },
                },
            },
            "b": {
                "index": ("public", "i_b"),
                "read": ("public", "r_b"),
                "engine": "pgroonga",
            },
        },
    }
    factory = ConfigurablePostgresFederatedSearch(config=cfg)
    with pytest.raises(CoreError, match="looks like an embedded hub"):
        factory(_federated_exec_context(), _fed())
