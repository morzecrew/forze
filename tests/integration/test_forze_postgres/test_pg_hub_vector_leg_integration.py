"""Integration tests for hub search with a ``vector`` engine leg (:class:`VectorHubLegEngine`)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.adapters.search._vector_sql import vector_param_literal
from forze_postgres.execution.deps.configs import PostgresHubSearchConfig
from forze_postgres.execution.deps.deps import ConfigurablePostgresHubSearch
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient
from forze_mock import MockHashEmbeddingsProvider


class VecItemFields(BaseModel):
    label: str


class VecHubLink(BaseModel):
    id: UUID
    item_id: UUID


def _embeddings_factory(
    _ctx: ExecutionContext,
    spec: EmbeddingsSpec,
) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hub_vector_leg_knn_single_and_multi_query(
    pgvector_client: PostgresClient,
) -> None:
    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")

    suffix = uuid4().hex[:8]
    items = f"hub_v_it_{suffix}"
    links = f"hub_v_lk_{suffix}"
    idx = f"idx_hub_v_{suffix}"

    await pgvector_client.execute(
        f"""
        CREATE TABLE {items} (
            id uuid PRIMARY KEY,
            label text NOT NULL,
            emb vector(3) NOT NULL
        );
        CREATE TABLE {links} (
            id uuid PRIMARY KEY,
            item_id uuid NOT NULL REFERENCES {items} (id)
        );
        CREATE INDEX {idx} ON {items} USING hnsw (emb vector_l2_ops);
        """
    )

    prov = MockHashEmbeddingsProvider(dimensions=3)
    i1, i2 = uuid4(), uuid4()
    v1 = vector_param_literal(await prov.embed_one("alpha"))
    v2 = vector_param_literal(await prov.embed_one("beta"))
    await pgvector_client.execute(
        f"""
        INSERT INTO {items} (id, label, emb) VALUES
        (%(a)s, 'a', '{v1}'::vector), (%(b)s, 'b', '{v2}'::vector)
        """,
        {"a": i1, "b": i2},
    )
    l1, l2 = uuid4(), uuid4()
    await pgvector_client.execute(
        f"""
        INSERT INTO {links} (id, item_id) VALUES (%(l1)s, %(i1)s), (%(l2)s, %(i2)s)
        """,
        {"l1": l1, "l2": l2, "i1": i1, "i2": i2},
    )

    leg_name = "vec_leg"
    leg_spec = SearchSpec(
        name=leg_name,
        model_type=VecItemFields,
        fields=["label"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_vec_{suffix}",
        model_type=VecHubLink,
        members=(leg_spec,),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", links),
        "members": {
            leg_name: {
                "index": ("public", idx),
                "read": ("public", items),
                "hub_fk": "item_id",
                "engine": "vector",
                "vector_column": "emb",
                "embedding_dimensions": 3,
                "embeddings_name": "hub_vec_emb",
            },
        },
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pgvector_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pgvector_client),
                EmbeddingsProviderDepKey: _embeddings_factory,
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    one = await adapter.search("alpha", return_count=True)
    assert one.count == 2
    assert one.hits[0].id == l1

    multi = await adapter.search(
        ["alpha", "beta"],
        options={"phrase_combine": "any"},
        return_count=True,
    )
    assert multi.count == 2

    browse = await adapter.search("", return_count=True)
    assert browse.count == 2
