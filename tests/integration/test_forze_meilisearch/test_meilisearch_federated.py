"""Integration tests for Meilisearch federated search."""

from __future__ import annotations

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
from forze.base.exceptions import CoreException
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
    full_spec = FederatedSearchSpec(name="fed_full", members=members)
    thin_spec = FederatedSearchSpec(name="fed_thin", members=members, thin_merge=True)

    full = await ctx.search.federated(full_spec).search_page(
        "zeta", pagination={"limit": 10}
    )
    thin = await ctx.search.federated(thin_spec).search_page(
        "zeta", pagination={"limit": 10}
    )

    def idents(page: object) -> list[tuple[str, str]]:
        return sorted((h.member, h.hit.id) for h in page.hits)  # type: ignore[attr-defined]

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
