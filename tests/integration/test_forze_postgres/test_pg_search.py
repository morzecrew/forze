from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.adapters.search import PostgresPGroongaSearchAdapterV2
from forze_postgres.execution.deps.configs import PostgresHubSearchConfig
from forze_postgres.execution.deps.deps import (
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class SearchableModel(BaseModel):
    id: UUID
    title: str
    content: str


@pytest.fixture
def execution_context(pg_client: PostgresClient):
    deps = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            SearchQueryDepKey: ConfigurablePostgresSearch(
                config={
                    "index": ("public", "idx_search_items_pgroonga"),
                    "read": ("public", "search_items"),
                    "engine": "pgroonga",
                }
            ),
        }
    )
    return ExecutionContext(deps=deps)


@pytest.mark.asyncio
async def test_postgres_search_adapter(
    pg_client: PostgresClient, execution_context: ExecutionContext
):
    # pgroonga is available in the image
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE search_items (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        """
    )

    # Create PGroonga index.
    # Must use ARRAY for multi-column search so it's matched as array by introspector
    await pg_client.execute(
        """
        CREATE INDEX idx_search_items_pgroonga
        ON search_items USING pgroonga ((ARRAY[title, content]));
        """
    )

    docs = [
        {
            "id": uuid4(),
            "title": "Forze Framework",
            "content": "Hexagonal architecture framework in python",
        },
        {
            "id": uuid4(),
            "title": "Postgres Guide",
            "content": "How to use postgres with python",
        },
        {
            "id": uuid4(),
            "title": "Python Tips",
            "content": "Advanced python development",
        },
    ]

    for doc in docs:
        await pg_client.execute(
            "INSERT INTO search_items (id, title, content) VALUES (%(id)s, %(title)s, %(content)s)",
            doc,
        )

    spec = SearchSpec(
        name="search_ns",
        model_type=SearchableModel,
        fields=["title", "content"],
    )

    adapter = execution_context.search_query(spec)

    res, cnt = await adapter.search("python")
    assert cnt == 3
    assert len(res) == 3

    res2, cnt2 = await adapter.search("hexagonal")
    assert cnt2 == 1
    assert len(res2) == 1
    assert res2[0].title == "Forze Framework"

    # Weighted search, pagination, explicit sort, and partial field projection
    page, total = await adapter.search(
        "python",
        pagination={"limit": 1, "offset": 0},
        sorts={"title": "asc"},
        options={"weights": {"title": 0.5, "content": 0.5}},
    )
    assert total == 3
    assert len(page) == 1

    titles_only, total_t = await adapter.search(
        "python",
        return_fields=["title"],
    )
    assert total_t == 3
    assert set(titles_only[0].keys()) == {"title"}


@pytest.mark.asyncio
async def test_postgres_pgroonga_search_adapter_v2_projection_vs_heap(
    pg_client: PostgresClient,
):
    """Index on heap columns; filters and projection use a view with aliased names."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE search_heap (
            id uuid PRIMARY KEY,
            doc_title text NOT NULL,
            doc_body text NOT NULL
        );
        """
    )

    await pg_client.execute(
        """
        CREATE VIEW search_projection AS
        SELECT id, doc_title AS title, doc_body AS content FROM search_heap;
        """
    )

    await pg_client.execute(
        """
        CREATE INDEX idx_search_heap_pgroonga
        ON search_heap USING pgroonga ((ARRAY[doc_title, doc_body]));
        """
    )

    docs = [
        {
            "id": uuid4(),
            "title": "Forze Framework",
            "content": "Hexagonal architecture framework in python",
        },
        {
            "id": uuid4(),
            "title": "Postgres Guide",
            "content": "How to use postgres with python",
        },
    ]

    for doc in docs:
        await pg_client.execute(
            (
                "INSERT INTO search_heap (id, doc_title, doc_body) "
                "VALUES (%(id)s, %(title)s, %(content)s)"
            ),
            doc,
        )

    introspector = PostgresIntrospector(client=pg_client)
    spec = SearchSpec(
        name="search_ns_v2",
        model_type=SearchableModel,
        fields=["title", "content"],
    )

    adapter = PostgresPGroongaSearchAdapterV2(
        spec=spec,
        source_qname=PostgresQualifiedName(schema="public", name="search_projection"),
        index_qname=PostgresQualifiedName(
            schema="public",
            name="idx_search_heap_pgroonga",
        ),
        index_heap_qname=PostgresQualifiedName(schema="public", name="search_heap"),
        client=pg_client,
        model_type=SearchableModel,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
        index_field_map={"title": "doc_title", "content": "doc_body"},
    )

    res, cnt = await adapter.search("python")
    assert cnt == 2
    assert {r.title for r in res} == {"Forze Framework", "Postgres Guide"}

    one, cnt_one = await adapter.search(
        "hexagonal",
        sorts={"title": "asc"},
    )
    assert cnt_one == 1
    assert one[0].title == "Forze Framework"


class LinkModel(BaseModel):
    id: UUID
    detail_id: UUID
    spec_id: UUID
    quantity: int


class _HubLegTxt(BaseModel):
    name: str = ""
    display_name: str = ""


@pytest.mark.asyncio
async def test_postgres_hub_pgroonga_search_links_or_legs(pg_client: PostgresClient):
    """OR across detail and spec heaps; filters on link table; return link rows."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE hub_details (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_specs (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_links (
            id uuid PRIMARY KEY,
            detail_id uuid NOT NULL REFERENCES hub_details (id),
            spec_id uuid NOT NULL REFERENCES hub_specs (id),
            quantity int NOT NULL
        );
        """
    )
    await pg_client.execute(
        """
        CREATE INDEX idx_hub_details_pg ON hub_details
        USING pgroonga ((ARRAY[name, display_name]));
        CREATE INDEX idx_hub_specs_pg ON hub_specs
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )

    d1, d2 = uuid4(), uuid4()
    s1, s2 = uuid4(), uuid4()
    await pg_client.execute(
        "INSERT INTO hub_details (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": d1, "name": "alpha detail", "dn": "Alpha D"},
    )
    await pg_client.execute(
        "INSERT INTO hub_details (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": d2, "name": "beta detail", "dn": "Beta D"},
    )
    await pg_client.execute(
        "INSERT INTO hub_specs (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": s1, "name": "gamma spec", "dn": "Gamma S"},
    )
    await pg_client.execute(
        "INSERT INTO hub_specs (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": s2, "name": "delta spec", "dn": "Delta S"},
    )
    lid1, lid2, lid3 = uuid4(), uuid4(), uuid4()
    await pg_client.execute(
        (
            "INSERT INTO hub_links (id, detail_id, spec_id, quantity) VALUES "
            "(%(a)s, %(d1)s, %(s1)s, 1), (%(b)s, %(d2)s, %(s1)s, 2), (%(c)s, %(d1)s, %(s2)s, 3)"
        ),
        {"a": lid1, "b": lid2, "c": lid3, "d1": d1, "d2": d2, "s1": s1, "s2": s2},
    )

    det_name = "detail_txt"
    spec_name = "spec_txt"

    detail_txt = SearchSpec(
        name=det_name,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    spec_txt = SearchSpec(
        name=spec_name,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name="hub_links_search",
        model_type=LinkModel,
        members=(detail_txt, spec_txt),
    )

    hub_pg: PostgresHubSearchConfig = {
        "hub": ("public", "hub_links"),
        "members": {
            det_name: {
                "index": ("public", "idx_hub_details_pg"),
                "read": ("public", "hub_details"),
                "hub_fk": "detail_id",
            },
            spec_name: {
                "index": ("public", "idx_hub_specs_pg"),
                "read": ("public", "hub_specs"),
                "hub_fk": "spec_id",
            },
        },
        "combine": "or",
        "score_merge": "max",
    }

    introspector = PostgresIntrospector(client=pg_client)
    ctx_hub = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: introspector,
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_pg)(ctx_hub, hub_spec)

    hits, cnt = await adapter.search("alpha")
    assert cnt == 2
    assert {h.id for h in hits} == {lid1, lid3}

    hits2, cnt2 = await adapter.search(
        "gamma",
        filters={"$fields": {"spec_id": str(s1)}},
    )
    assert cnt2 == 2
    assert {h.id for h in hits2} == {lid1, lid2}

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: introspector,
                HubSearchQueryDepKey: ConfigurablePostgresHubSearch(config=hub_pg),
            }
        )
    )
    resolved = ctx.hub_search_query(hub_spec)
    same, c3 = await resolved.search("delta")

    assert c3 == 1
    assert same[0].id == lid3
