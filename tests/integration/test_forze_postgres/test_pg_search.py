from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.adapters.search import PostgresPGroongaSearchAdapter
from forze_postgres.execution.deps import (
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    PostgresSearchConfig,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


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
                config=PostgresSearchConfig(
                    index=("public", "idx_search_items_pgroonga"),
                    read=("public", "search_items"),
                    engine="pgroonga",
                )
            ),
        }
    )
    return context_from_deps(deps)


@pytest.mark.asyncio
async def test_postgres_pgroonga_single_column_index(
    pg_client: PostgresClient,
) -> None:
    """PGroonga index on one column (non-ARRAY expression) exercises heap match path."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE pg1col_docs (
            id uuid PRIMARY KEY,
            title text NOT NULL
        );
        CREATE INDEX idx_pg1col_title ON pg1col_docs USING pgroonga (title);
        """
    )
    await pg_client.execute(
        "INSERT INTO pg1col_docs (id, title) VALUES (%(id)s, %(t)s)",
        {"id": uuid4(), "t": "singleton pgroonga row"},
    )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", "idx_pg1col_title"),
                        read=("public", "pg1col_docs"),
                        engine="pgroonga",
                    )
                ),
            }
        )
    )

    class OneCol(BaseModel):
        id: UUID
        title: str

    spec = SearchSpec(name="pg1col", model_type=OneCol, fields=["title"])
    adapter = ctx.search.query(spec)
    __p = await adapter.search_page("singleton", options={"fuzzy": True})
    rows = __p.hits
    n = __p.count
    assert n == 1
    assert rows[0].title == "singleton pgroonga row"


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

    adapter = execution_context.search.query(spec)

    assert isinstance(adapter, PostgresPGroongaSearchAdapter)

    __p = await adapter.search_page("python")
    res = __p.hits
    cnt = __p.count
    assert cnt == 3
    assert len(res) == 3

    __p = await adapter.search_page("hexagonal")
    res2 = __p.hits
    cnt2 = __p.count
    assert cnt2 == 1
    assert len(res2) == 1
    assert res2[0].title == "Forze Framework"

    class TitleOnly(BaseModel):
        title: str

    __p = await adapter.select_search_page(TitleOnly, "python")
    as_titles = __p.hits
    n_t = __p.count
    assert n_t == 3
    assert {r.title for r in as_titles} == {d["title"] for d in docs}

    __p = await adapter.search_page("zznonexistent999")
    none_rows = __p.hits
    n_none = __p.count
    assert n_none == 0
    assert none_rows == []

    await adapter.search_page("python", options={"fuzzy": True})

    # Weighted search, pagination, explicit sort, and partial field projection
    __p = await adapter.search_page(
        "python",
        pagination={"limit": 1, "offset": 0},
        sorts={"title": "asc"},
        options={"weights": {"title": 0.5, "content": 0.5}},
    )
    page = __p.hits
    total = __p.count
    assert total == 3
    assert len(page) == 1

    __p = await adapter.project_search_page(["title"], "python")
    titles_only = __p.hits
    total_t = __p.count
    assert total_t == 3
    assert set(titles_only[0].keys()) == {"title"}

    __p = await adapter.search_page(["python", "framework"])
    n_any = __p.count
    assert n_any == 3
    __p = await adapter.search_page(
        ["python", "framework"], options={"phrase_combine": "all"}
    )
    all_two = __p.hits
    n_all = __p.count
    assert n_all == 1
    assert all_two[0].title == "Forze Framework"


@pytest.mark.asyncio
async def test_pgroonga_search_spec_field_order_does_not_change_ranking(
    pg_client: PostgresClient,
) -> None:
    """``SearchSpec.fields`` order is ignored; weights stay tied to logical field names."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:12]
    table = f"pg_order_{suffix}"
    index_name = f"idx_pg_order_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        CREATE INDEX {index_name}
        ON {table} USING pgroonga ((ARRAY[title, content]));
        """
    )

    doc_title_match = {
        "id": uuid4(),
        "title": "alpha alpha",
        "content": "minor",
    }
    doc_content_match = {
        "id": uuid4(),
        "title": "minor",
        "content": "alpha alpha",
    }
    for row in (doc_title_match, doc_content_match):
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(title)s, %(content)s)
            """,
            row,
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index_name),
                        read=("public", table),
                        engine="pgroonga",
                    )
                ),
            }
        )
    )

    weights = {"title": 1.0, "content": 0.5}
    spec_canonical = SearchSpec(
        name="pg_order_canonical",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    spec_reversed = SearchSpec(
        name="pg_order_reversed",
        model_type=SearchableModel,
        fields=["content", "title"],
    )

    page_canonical = await ctx.search.query(spec_canonical).search_page(
        "alpha",
        options={"weights": weights},
    )
    page_reversed = await ctx.search.query(spec_reversed).search_page(
        "alpha",
        options={"weights": weights},
    )

    assert page_canonical.count == 2
    assert page_reversed.count == 2
    assert [h.id for h in page_canonical.hits] == [h.id for h in page_reversed.hits]
    assert page_canonical.hits[0].id == doc_title_match["id"]


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

    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        relation=("public", "search_projection"),
        index_relation=("public", "idx_search_heap_pgroonga"),
        index_heap_relation=("public", "search_heap"),
        client=pg_client,
        model_type=SearchableModel,
        codec=spec.resolved_read_codec,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
        index_field_map={"title": "doc_title", "content": "doc_body"},
    )

    __p = await adapter.search_page("python")
    res = __p.hits
    cnt = __p.count
    assert cnt == 2
    assert {r.title for r in res} == {"Forze Framework", "Postgres Guide"}

    __p = await adapter.search_page("hexagonal", sorts={"title": "asc"})
    one = __p.hits
    cnt_one = __p.count
    assert cnt_one == 1
    assert one[0].title == "Forze Framework"


@pytest.mark.asyncio
async def test_postgres_search_configurable_uses_heap_and_field_map(
    pg_client: PostgresClient,
) -> None:
    """``ConfigurablePostgresSearch`` + ``heap`` / ``field_map`` for PGroonga v2."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE cfg_heap (
            id uuid PRIMARY KEY,
            t1 text NOT NULL,
            t2 text NOT NULL
        );
        CREATE VIEW cfg_proj AS SELECT id, t1 AS title, t2 AS content FROM cfg_heap;
        CREATE INDEX idx_cfg_pg ON cfg_heap USING pgroonga ((ARRAY[t1, t2]));
        """
    )
    await pg_client.execute(
        "INSERT INTO cfg_heap (id, t1, t2) VALUES (%(id)s, 'hello', 'world')",
        {"id": uuid4()},
    )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", "idx_cfg_pg"),
                        read=("public", "cfg_proj"),
                        heap=("public", "cfg_heap"),
                        engine="pgroonga",
                        field_map={"title": "t1", "content": "t2"},
                    )
                ),
            }
        )
    )
    spec = SearchSpec(
        name="cfg_ns",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    adapter = ctx.search.query(spec)
    assert isinstance(adapter, PostgresPGroongaSearchAdapter)

    __p = await adapter.search_page("hello")
    rows = __p.hits
    n = __p.count
    assert n == 1
    assert rows[0].title == "hello"


@pytest.mark.asyncio
async def test_postgres_pgroonga_index_first_plan(
    pg_client: PostgresClient,
) -> None:
    """``pgroonga_plan='index_first'`` returns ranked hits on a coalesced table."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE idx1_docs (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        CREATE INDEX idx_idx1_docs ON idx1_docs USING pgroonga ((ARRAY[title, content]));
        """
    )
    ids = [uuid4(), uuid4()]
    await pg_client.execute(
        "INSERT INTO idx1_docs (id, title, content) VALUES (%(id)s, %(t)s, %(c)s)",
        {"id": ids[0], "t": "alpha bravo", "c": "first"},
    )
    await pg_client.execute(
        "INSERT INTO idx1_docs (id, title, content) VALUES (%(id)s, %(t)s, %(c)s)",
        {"id": ids[1], "t": "bravo charlie", "c": "second"},
    )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", "idx_idx1_docs"),
                        read=("public", "idx1_docs"),
                        engine="pgroonga",
                        pgroonga_plan="index_first",
                        pgroonga_candidate_limit=10,
                    )
                ),
            }
        )
    )
    spec = SearchSpec(
        name="idx1",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    page = await ctx.search.query(spec).search_page("bravo", pagination={"limit": 5})
    assert len(page.hits) == 2
    assert {h.title for h in page.hits} == {"alpha bravo", "bravo charlie"}


@pytest.mark.asyncio
async def test_pgroonga_exact_total_exceeds_candidate_cap(
    pg_client: PostgresClient,
) -> None:
    """Capped ranked heap still reports full exact total via uncapped COUNT."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:10]
    table = f"cap_docs_{suffix}"
    index_name = f"idx_cap_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL
        );
        CREATE INDEX {index_name} ON {table}
        USING pgroonga ((ARRAY[title, content]));
        """
    )
    token = "capmatch"
    for i in range(15):
        await pg_client.execute(
            f"""
            INSERT INTO {table} (id, title, content)
            VALUES (%(id)s, %(t)s, %(c)s)
            """,
            {
                "id": uuid4(),
                "t": f"{token} {i}",
                "c": "body",
            },
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index_name),
                        read=("public", table),
                        engine="pgroonga",
                        pgroonga_plan="filter_first",
                        pgroonga_candidate_limit=5,
                    )
                ),
            }
        )
    )
    spec = SearchSpec(
        name="cap",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    page = await ctx.search.query(spec).search_page(
        token,
        pagination={"limit": 3},
        options={"search_count": "exact"},
    )
    assert page.count == 15
    assert len(page.hits) == 3
