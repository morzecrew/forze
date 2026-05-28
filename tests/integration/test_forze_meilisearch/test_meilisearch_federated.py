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
from forze_meilisearch.execution.deps import MeilisearchClientDepKey
from forze_meilisearch.execution.deps.deps import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearchCommand,
)

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
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config={
                        "merge": "federation",
                        "members": {
                            "a": {"index_uid": "fed_a"},
                            "b": {"index_uid": "fed_b"},
                        },
                    }
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config={"index_uid": "unused"},
                ),
            }
        )
    )

    for member, uid in (("a", "fed_a"), ("b", "fed_b")):
        cmd = ConfigurableMeilisearchSearchCommand(
            config={"index_uid": uid},
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
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MeilisearchClientDepKey: meilisearch_client,
                FederatedSearchQueryDepKey: ConfigurableMeilisearchFederatedSearch(
                    config={
                        "merge": "rrf",
                        "members": {
                            "a": {"index_uid": "rrf_a"},
                            "b": {"index_uid": "rrf_b"},
                        },
                    }
                ),
                SearchCommandDepKey: ConfigurableMeilisearchSearchCommand(
                    config={"index_uid": "unused"},
                ),
            }
        )
    )

    for member, uid in (("a", "rrf_a"), ("b", "rrf_b")):
        cmd = ConfigurableMeilisearchSearchCommand(
            config={"index_uid": uid},
        )(ctx, _mem(member))
        await cmd.ensure_index()
        await cmd.delete_all()
        await cmd.upsert([Hit(id="1", label=f"{member}-beta")])

    page = await ctx.search.federated(spec).search_page("beta")
    assert page.count >= 1
