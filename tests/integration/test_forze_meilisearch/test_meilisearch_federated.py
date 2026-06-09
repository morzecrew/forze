"""Integration tests for Meilisearch federated search."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    SearchCommandDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearchCommand,
    MeilisearchClientDepKey,
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
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
            }
        )
    )

    for member, uid in (("a", "fed_a"), ("b", "fed_b")):
        cmd = ConfigurableMeilisearchSearchCommand(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        await cmd.ensure_index()
        await cmd.delete_all()
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
            }
        )
    )

    for member, uid in (("a", "rrf_a"), ("b", "rrf_b")):
        cmd = ConfigurableMeilisearchSearchCommand(
            config=MeilisearchSearchConfig(index_uid=uid),
        )(ctx, _mem(member))
        await cmd.ensure_index()
        await cmd.delete_all()
        await cmd.upsert([Hit(id="1", label=f"{member}-beta")])

    page = await ctx.search.federated(spec).search_page("beta")
    assert page.count >= 1


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
            },
        ),
    )

    for member, uid in (("a", "fed_adv_a"), ("b", "fed_adv_b")):
        cmd = ConfigurableMeilisearchSearchCommand(config=member_cfg[member])(
            ctx,
            _mem(member),
        )
        await cmd.ensure_index()
        await cmd.delete_all()
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
