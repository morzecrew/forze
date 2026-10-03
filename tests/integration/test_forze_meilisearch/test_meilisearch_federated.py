"""Integration tests for Meilisearch federated search."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchSpec,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.factories import (
    ConfigurableMeilisearchSearchManagement,
)
from tests.support.execution_context import context_from_deps

# ----------------------- #


class Hit(BaseModel):
    id: str
    label: str


def _mem(name: str) -> SearchSpec[Hit]:
    return SearchSpec(name=name, model_type=Hit, fields=["label"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_federation_merge(meilisearch_client) -> None:
    spec = FederatedSearchSpec(name="fed", members=(_mem("a"), _mem("b")))
    ctx = context_from_deps(Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(
                        merge="federation",
                        members={
                            "a": MeilisearchSearchConfig(index_uid="fed_a"),
                            "b": MeilisearchSearchConfig(index_uid="fed_b"),
                        },
                    ),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
            }
        )
    )

    for member, uid in (("a", "fed_a"), ("b", "fed_b")):
        cmd = ConfigurableMeilisearchSearchCommand(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        mgmt = ConfigurableMeilisearchSearchManagement(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await cmd.upsert([Hit(id="1", label=f"{member}-alpha")])

    page = await ctx.search.federated(spec).search_page("alpha")
    assert page.count >= 1
    members = {h.member for h in page.hits}
    assert members <= {"a", "b"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_rrf_merge(meilisearch_client) -> None:
    spec = FederatedSearchSpec(name="fed_rrf", members=(_mem("a"), _mem("b")))
    ctx = context_from_deps(Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(
                        merge="rrf",
                        members={
                            "a": MeilisearchSearchConfig(index_uid="rrf_a"),
                            "b": MeilisearchSearchConfig(index_uid="rrf_b"),
                        },
                    ),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
            }
        )
    )

    for member, uid in (("a", "rrf_a"), ("b", "rrf_b")):
        cmd = ConfigurableMeilisearchSearchCommand(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        mgmt = ConfigurableMeilisearchSearchManagement(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await cmd.upsert([Hit(id="1", label=f"{member}-beta")])

    page = await ctx.search.federated(spec).search_page("beta")
    assert page.count >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_rrf_thin_merge_matches_full(meilisearch_client) -> None:
    """``thin_merge=True`` returns the same federated hits as the full-fetch path."""
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(
                        merge="rrf",
                        members={
                            "a": MeilisearchSearchConfig(index_uid="thin_a"),
                            "b": MeilisearchSearchConfig(index_uid="thin_b"),
                        },
                    ),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
            }
        )
    )

    docs = {
        ("a", "thin_a"): [Hit(id="1", label="zeta shared"), Hit(id="2", label="zeta a")],
        ("b", "thin_b"): [Hit(id="1", label="zeta shared"), Hit(id="3", label="zeta b")],
    }
    for (member, uid), member_docs in docs.items():
        cmd = ConfigurableMeilisearchSearchCommand(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        mgmt = ConfigurableMeilisearchSearchManagement(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await cmd.upsert(member_docs)

    members = (_mem("a"), _mem("b"))
    full_spec = FederatedSearchSpec(name="fed_full", members=members, thin_merge=False)
    thin_spec = FederatedSearchSpec(name="fed_thin", members=members, thin_merge=True)

    full = await ctx.search.federated(full_spec).search_page(
        "zeta", pagination={"limit": 10}
    )
    thin = await ctx.search.federated(thin_spec).search_page(
        "zeta", pagination={"limit": 10}
    )

    def idents(page: object) -> list[tuple[str, str]]:
        return [(h.member, h.hit.id) for h in page.hits]  # type: ignore[attr-defined]

    assert idents(thin) == idents(full)
    assert thin.count == full.count
    assert ("a", "1") in idents(thin) and ("b", "1") in idents(thin)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_with_filters_and_cursor(meilisearch_client) -> None:
    spec = FederatedSearchSpec(name="fed_adv", members=(_mem("a"), _mem("b")))
    member_cfg = {
        "a": MeilisearchSearchConfig(
            index_uid="fed_adv_a",
            filterable_attributes=["label"],
            sortable_attributes=["label"],
        ),
        "b": MeilisearchSearchConfig(
            index_uid="fed_adv_b",
            filterable_attributes=["label"],
            sortable_attributes=["label"],
        ),
    }
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(
                        merge="rrf",
                        members=member_cfg,
                    ),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
            },
        ),
    )

    for member, uid in (("a", "fed_adv_a"), ("b", "fed_adv_b")):
        cmd = ConfigurableMeilisearchSearchCommand(config=member_cfg[member])(
            ctx,
            _mem(member),
        )
        mgmt = ConfigurableMeilisearchSearchManagement(config=member_cfg[member])(
            ctx,
            _mem(member),
        )
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await cmd.upsert(
            [
                Hit(id="1", label=f"{member}-match"),
                Hit(id="2", label=f"{member}-other"),
            ],
        )

    fed = ctx.search.federated(spec)
    page = await fed.search_page(
        "match",
        filters={"$values": {"label": {"$neq": "nope"}}},
        sorts={"label": "asc"},
        pagination={"offset": 0, "limit": 10},
    )
    assert page.count >= 1

    with pytest.raises(CoreException, match="search_cursor is not implemented"):
        await fed.search_cursor("match", cursor={"limit": 1})


def _fed_ctx(meilisearch_client, *, merge: str, a_uid: str, b_uid: str):
    return context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(
                        merge=merge,  # type: ignore[arg-type]
                        members={
                            "a": MeilisearchSearchConfig(index_uid=a_uid),
                            "b": MeilisearchSearchConfig(index_uid=b_uid),
                        },
                    ),
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
                SearchManagementDepKey: ConfigurableMeilisearchSearchManagement(
                    config=MeilisearchSearchConfig(index_uid="unused"),
                ),
            }
        )
    )


async def _seed_fed(ctx, *, a_uid: str, b_uid: str) -> None:
    for member, uid in (("a", a_uid), ("b", b_uid)):
        cmd = ConfigurableMeilisearchSearchCommand(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        mgmt = ConfigurableMeilisearchSearchManagement(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await cmd.upsert([Hit(id="1", label=f"{member} shared book")])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_rrf_highlights(meilisearch_client) -> None:
    spec = FederatedSearchSpec(name="fed_hl", members=(_mem("a"), _mem("b")))
    ctx = _fed_ctx(meilisearch_client, merge="rrf", a_uid="fed_hl_a", b_uid="fed_hl_b")
    await _seed_fed(ctx, a_uid="fed_hl_a", b_uid="fed_hl_b")

    page = await ctx.search.federated(spec).search_page(
        "book", options={"highlight": {"fields": ["label"]}}
    )

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    fragments = [hl["label"][0] for hl in page.highlights if "label" in hl]
    assert fragments
    assert all("<em>" in frag.lower() and "book" in frag.lower() for frag in fragments)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_native_highlights_fail_closed(meilisearch_client) -> None:
    spec = FederatedSearchSpec(name="fed_hl_native", members=(_mem("a"), _mem("b")))
    ctx = _fed_ctx(
        meilisearch_client, merge="federation", a_uid="fed_hln_a", b_uid="fed_hln_b"
    )
    await _seed_fed(ctx, a_uid="fed_hln_a", b_uid="fed_hln_b")

    with pytest.raises(CoreException, match="native federation"):
        await ctx.search.federated(spec).search_page(
            "book", options={"highlight": True}
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_facets_fail_closed(meilisearch_client) -> None:
    spec = FederatedSearchSpec(name="fed_facet", members=(_mem("a"), _mem("b")))
    ctx = _fed_ctx(meilisearch_client, merge="rrf", a_uid="fed_fc_a", b_uid="fed_fc_b")
    await _seed_fed(ctx, a_uid="fed_fc_a", b_uid="fed_fc_b")

    with pytest.raises(CoreException, match="does not support facets"):
        await ctx.search.federated(spec).search_page(
            "book", options={"facets": ["label"]}
        )


class Ranked(BaseModel):
    id: str
    label: str
    rank: int


def _ranked(name: str) -> SearchSpec[Ranked]:
    return SearchSpec(name=name, model_type=Ranked, fields=["label"], default_sort={"rank": "asc"})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_native_blank_query_orders_by_the_members_default_sort(
    meilisearch_client,
) -> None:
    # A blank query has no relevance: as on one index, each member sends its default_sort,
    # then its primary key, and the federation merges the members in that order.
    ctx = _fed_ctx(
        meilisearch_client, merge="federation", a_uid="fed_blank_a", b_uid="fed_blank_b"
    )
    rows = {"a": [("1", 30), ("2", 10), ("3", 50)], "b": [("4", 20), ("5", 40), ("6", 5)]}

    for member, uid in (("a", "fed_blank_a"), ("b", "fed_blank_b")):
        config = MeilisearchSearchConfig(index_uid=uid)
        mgmt = ConfigurableMeilisearchSearchManagement(config=config)(ctx, _ranked(member))
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await ConfigurableMeilisearchSearchCommand(config=config)(ctx, _ranked(member)).upsert(
            [Ranked(id=i, label="row", rank=r) for i, r in rows[member]]
        )

    spec = FederatedSearchSpec(name="fed_blank", members=(_ranked("a"), _ranked("b")))
    page = await ctx.search.federated(spec).search_page("", pagination={"limit": 10})

    assert [row.hit.rank for row in page.hits] == [5, 10, 20, 30, 40, 50]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_native_blank_query_on_a_stale_index_is_a_configuration_error(
    meilisearch_client,
) -> None:
    # Provisioned before the spec set a default_sort: the index cannot sort by it.
    ctx = _fed_ctx(
        meilisearch_client, merge="federation", a_uid="fed_stale_a", b_uid="fed_stale_b"
    )

    for member, uid in (("a", "fed_stale_a"), ("b", "fed_stale_b")):
        config = MeilisearchSearchConfig(index_uid=uid)
        old = SearchSpec(name=member, model_type=Ranked, fields=["label"])
        mgmt = ConfigurableMeilisearchSearchManagement(config=config)(ctx, old)
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await ConfigurableMeilisearchSearchCommand(config=config)(ctx, old).upsert(
            [Ranked(id=f"{member}1", label="row", rank=1)]
        )

    spec = FederatedSearchSpec(name="fed_stale", members=(_ranked("a"), _ranked("b")))

    with pytest.raises(CoreException) as refused:
        await ctx.search.federated(spec).search_page("", pagination={"limit": 10})

    assert refused.value.kind is ExceptionKind.CONFIGURATION
    assert "ensure_index" in str(refused.value)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("merge", ["federation", "rrf"])
@pytest.mark.parametrize(
    "case",
    [
        "mixed_sorts",
        "one_default",
        "custom_pk",
        "custom_pk_same_sort",
        "pinned_no_pk",
        "pinned_sortable_no_pk",
    ],
)
async def test_federated_members_that_disagree_still_answer(
    meilisearch_client, merge: str, case: str
) -> None:
    # Members whose orders or keys differ: Meilisearch refuses a federation whose queries sort
    # differently, and a custom primary key leaves ``id`` unfilterable for the thin re-read.
    sfx = uuid4().hex[:6]
    configs = {
        "a": MeilisearchSearchConfig(index_uid=f"dis_a_{sfx}"),
        "b": MeilisearchSearchConfig(index_uid=f"dis_b_{sfx}"),
    }
    specs = {
        "a": SearchSpec(name="a", model_type=Ranked, fields=["label"], default_sort={"rank": "asc"}),
        "b": SearchSpec(
            name="b", model_type=Ranked, fields=["label"], default_sort={"label": "desc"}
        ),
    }

    if case == "one_default":
        specs["b"] = SearchSpec(name="b", model_type=Ranked, fields=["label"])
    if case in ("custom_pk", "custom_pk_same_sort"):
        configs["b"] = MeilisearchSearchConfig(index_uid=f"dis_b_{sfx}", primary_key="doc_id")
    if case == "custom_pk_same_sort":
        specs["b"] = SearchSpec(
            name="b", model_type=Ranked, fields=["label"], default_sort={"rank": "asc"}
        )
    if case == "pinned_no_pk":
        configs["b"] = MeilisearchSearchConfig(
            index_uid=f"dis_b_{sfx}", filterable_attributes=["label"]
        )
    if case == "pinned_sortable_no_pk":
        configs["b"] = MeilisearchSearchConfig(
            index_uid=f"dis_b_{sfx}", sortable_attributes=["label"]
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(
                        merge=merge,  # type: ignore[arg-type]
                        members=configs,
                    ),
                ),
            }
        )
    )

    for member in ("a", "b"):
        mgmt = ConfigurableMeilisearchSearchManagement(config=configs[member])(ctx, specs[member])
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await ConfigurableMeilisearchSearchCommand(config=configs[member])(
            ctx, specs[member]
        ).upsert(
            [
                Ranked(id=f"{member}{i}", label=f"row {chr(97 + i)}", rank=(i * 7) % 5)
                for i in range(6)
            ]
        )

    spec = FederatedSearchSpec(name=f"dis_{sfx}", members=(specs["a"], specs["b"]))
    everyone = {(m, f"{m}{i}") for m in ("a", "b") for i in range(6)}

    for query in ("", "row"):
        page = await ctx.search.federated(spec).search_page(query, pagination={"limit": 20})

        assert {(row.member, row.hit.id) for row in page.hits} == everyone


@pytest.mark.integration
@pytest.mark.asyncio
async def test_federated_rrf_thin_pages_match_full_pages_in_order(meilisearch_client) -> None:
    """Every page of a thin merge holds the full merge's rows, in its order, deep pages too."""
    sfx = uuid4().hex[:6]
    configs = {m: MeilisearchSearchConfig(index_uid=f"tvf_{m}_{sfx}") for m in ("a", "b")}
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(merge="rrf", members=configs),
                ),
            }
        )
    )

    for member, config in configs.items():
        spec = _ranked(member)
        mgmt = ConfigurableMeilisearchSearchManagement(config=config)(ctx, spec)
        await mgmt.ensure_index()
        await mgmt.delete_all()
        # Relevance repeats every 7 rows, so ties span members and pages.
        await ConfigurableMeilisearchSearchCommand(config=config)(ctx, spec).upsert(
            [
                Ranked(id=f"{member}{i:03}", label=" ".join(["tok"] * (i % 7 + 1)), rank=i)
                for i in range(120)
            ]
        )

    members = (_ranked("a"), _ranked("b"))
    full = ctx.search.federated(
        FederatedSearchSpec(name=f"tvf_full_{sfx}", members=members, thin_merge=False)
    )
    thin = ctx.search.federated(FederatedSearchSpec(name=f"tvf_thin_{sfx}", members=members))

    for sorts in (None, {"rank": "desc"}):
        for offset in (0, 20, 60, 100, 150, 200, 230):
            window = {"limit": 10, "offset": offset}
            want = await full.search_page("tok", pagination=window, sorts=sorts)
            got = await thin.search_page("tok", pagination=window, sorts=sorts)

            assert [(h.member, h.hit.id) for h in got.hits] == [
                (h.member, h.hit.id) for h in want.hits
            ], (sorts, offset)
            assert got.count == want.count == 240


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("merge", ["federation", "rrf"])
async def test_federated_text_query_on_an_index_older_than_its_default_sort(
    meilisearch_client, merge: str
) -> None:
    # A query with text never sorts by default_sort, so an index provisioned before the spec
    # set one still answers it — including the thin merge's re-read of the page.
    sfx = uuid4().hex[:6]
    configs = {m: MeilisearchSearchConfig(index_uid=f"old_{m}_{sfx}") for m in ("a", "b")}
    ctx = context_from_deps(
        Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config=MeilisearchFederatedSearchConfig(
                        merge=merge,  # type: ignore[arg-type]
                        members=configs,
                    ),
                ),
            }
        )
    )

    for member, config in configs.items():
        old = SearchSpec(name=member, model_type=Ranked, fields=["label"])
        mgmt = ConfigurableMeilisearchSearchManagement(config=config)(ctx, old)
        await mgmt.ensure_index()
        await mgmt.delete_all()
        await ConfigurableMeilisearchSearchCommand(config=config)(ctx, old).upsert(
            [Ranked(id=f"{member}{i}", label=f"row {i}", rank=i) for i in range(4)]
        )

    spec = FederatedSearchSpec(name=f"old_{sfx}", members=(_ranked("a"), _ranked("b")))
    page = await ctx.search.federated(spec).search_page("row", pagination={"limit": 20})

    assert page.count == 8
    assert len(page.hits) == 8
