from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, CursorPage, Page
from forze.application.contracts.query import QueryFilterExpression
from forze.application.contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze_postgres.adapters.search import PostgresPGroongaSearchAdapter
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

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", "idx_pg1col_title"),
                        "read": ("public", "pg1col_docs"),
                        "engine": "pgroonga",
                    }
                ),
            }
        )
    )

    class OneCol(BaseModel):
        id: UUID
        title: str

    spec = SearchSpec(name="pg1col", model_type=OneCol, fields=["title"])
    adapter = ctx.search_query(spec)
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

    adapter = execution_context.search_query(spec)

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
    __p = await adapter.search_page(["python", "framework"], options={"phrase_combine": "all"})
    all_two = __p.hits
    n_all = __p.count
    assert n_all == 1
    assert all_two[0].title == "Forze Framework"


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

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config={
                        "index": ("public", "idx_cfg_pg"),
                        "read": ("public", "cfg_proj"),
                        "heap": ("public", "cfg_heap"),
                        "engine": "pgroonga",
                        "field_map": {"title": "t1", "content": "t2"},
                    }
                ),
            }
        )
    )
    spec = SearchSpec(
        name="cfg_ns",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    adapter = ctx.search_query(spec)
    assert isinstance(adapter, PostgresPGroongaSearchAdapter)

    __p = await adapter.search_page("hello")
    rows = __p.hits
    n = __p.count
    assert n == 1
    assert rows[0].title == "hello"


class LinkModel(BaseModel):
    id: UUID
    detail_id: UUID
    spec_id: UUID
    quantity: int


class _HubLegTxt(BaseModel):
    name: str = ""
    display_name: str = ""


class ContractMultiFkModel(BaseModel):
    id: UUID
    party_a_id: UUID
    party_b_id: UUID
    label_id: UUID


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

    __p = await adapter.search_page("alpha")
    hits = __p.hits
    cnt = __p.count
    assert cnt == 2
    assert {h.id for h in hits} == {lid1, lid3}

    __p = await adapter.search_page("alpha", sorts={"quantity": "asc"})
    sorted_by_qty = __p.hits
    cnt_sort = __p.count
    assert cnt_sort == 2
    assert [h.quantity for h in sorted_by_qty] == [1, 3]

    __p = await adapter.search_page("alpha", pagination={"limit": 1, "offset": 0})
    page1 = __p.hits
    cnt_page = __p.count
    assert cnt_page == 2
    assert len(page1) == 1

    class LinkIdQty(BaseModel):
        id: UUID
        quantity: int

    __p = await adapter.select_search_page(LinkIdQty, "alpha")
    partial = __p.hits
    cnt_partial = __p.count
    assert cnt_partial == 2
    assert {p.id for p in partial} == {lid1, lid3}
    assert all(isinstance(p.quantity, int) for p in partial)

    __p = await adapter.project_search_page(
        ["id", "quantity"],
        "alpha",
    )
    raw_links = __p.hits
    cnt_raw = __p.count
    assert cnt_raw == 2
    assert {r["id"] for r in raw_links} == {lid1, lid3}

    hub_pg_sum: PostgresHubSearchConfig = {**hub_pg, "score_merge": "sum"}
    adapter_sum = ConfigurablePostgresHubSearch(config=hub_pg_sum)(ctx_hub, hub_spec)
    __p = await adapter_sum.search_page("alpha")
    sum_hits = __p.hits
    sum_cnt = __p.count
    assert sum_cnt == 2
    assert {h.id for h in sum_hits} == {lid1, lid3}

    __p = await adapter.search_page(
        "alpha",
        options={"member_weights": {det_name: 0.0, spec_name: 0.0}},
    )
    all_legs_off = __p.hits
    n_off = __p.count
    assert n_off == 3
    assert {h.id for h in all_legs_off} == {lid1, lid2, lid3}

    __p = await adapter.search_page("no_such_term_xyz")
    _ = __p.hits
    n_no_match = __p.count
    assert n_no_match == 0

    __p = await adapter.search_page(
        "gamma", filters={"$fields": {"spec_id": str(s1)}}
    )
    hits2 = __p.hits
    cnt2 = __p.count
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
    __p = await resolved.search_page("delta")
    same = __p.hits
    c3 = __p.count
    assert c3 == 1
    assert same[0].id == lid3

    __p = await adapter.search_page("")
    browse = __p.hits
    c_browse = __p.count
    assert c_browse == 3
    assert {h.id for h in browse} == {lid1, lid2, lid3}

    __p = await adapter.search_page("   \t")
    c_ws = __p.count
    assert c_ws == 3

    __p = await adapter.search_page(
        "alpha",
        options={"member_weights": {det_name: 0.0, spec_name: 1.0}},
    )
    c_z = __p.count
    assert c_z == 0

    __p = await adapter.search_page("gamma", options={"members": [spec_name]})
    only_spec = __p.hits
    c_os = __p.count
    assert c_os == 2
    assert {h.id for h in only_spec} == {lid1, lid2}


@pytest.mark.asyncio
async def test_postgres_hub_fts_search_links_or_legs(pg_client: PostgresClient) -> None:
    """OR across two GIN ``tsvector`` heaps (FTS hub legs); filters on link table."""

    await pg_client.execute(
        """
        CREATE TABLE hub_fts_details (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_fts_specs (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_fts_links (
            id uuid PRIMARY KEY,
            detail_id uuid NOT NULL REFERENCES hub_fts_details (id),
            spec_id uuid NOT NULL REFERENCES hub_fts_specs (id),
            quantity int NOT NULL
        );
        """
    )
    await pg_client.execute(
        """
        CREATE INDEX idx_hub_fts_details_gin ON hub_fts_details
        USING gin (to_tsvector('english', coalesce(name, '') || ' ' || coalesce(display_name, '')));
        CREATE INDEX idx_hub_fts_specs_gin ON hub_fts_specs
        USING gin (to_tsvector('english', coalesce(name, '') || ' ' || coalesce(display_name, '')));
        """
    )

    d1, d2 = uuid4(), uuid4()
    s1, s2 = uuid4(), uuid4()
    await pg_client.execute(
        "INSERT INTO hub_fts_details (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": d1, "name": "alpha detail", "dn": "Alpha D"},
    )
    await pg_client.execute(
        "INSERT INTO hub_fts_details (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": d2, "name": "beta detail", "dn": "Beta D"},
    )
    await pg_client.execute(
        "INSERT INTO hub_fts_specs (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": s1, "name": "gamma spec", "dn": "Gamma S"},
    )
    await pg_client.execute(
        "INSERT INTO hub_fts_specs (id, name, display_name) VALUES (%(id)s, %(name)s, %(dn)s)",
        {"id": s2, "name": "delta spec", "dn": "Delta S"},
    )
    lid1, lid2, lid3 = uuid4(), uuid4(), uuid4()
    await pg_client.execute(
        (
            "INSERT INTO hub_fts_links (id, detail_id, spec_id, quantity) VALUES "
            "(%(a)s, %(d1)s, %(s1)s, 1), (%(b)s, %(d2)s, %(s1)s, 2), (%(c)s, %(d1)s, %(s2)s, 3)"
        ),
        {"a": lid1, "b": lid2, "c": lid3, "d1": d1, "d2": d2, "s1": s1, "s2": s2},
    )

    det_name = "detail_fts"
    spec_name = "spec_fts"
    fts_groups = {"A": ("name",), "B": ("display_name",)}

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
        name="hub_fts_links_search",
        model_type=LinkModel,
        members=(detail_txt, spec_txt),
    )

    hub_fts_cfg: PostgresHubSearchConfig = {
        "hub": ("public", "hub_fts_links"),
        "members": {
            det_name: {
                "index": ("public", "idx_hub_fts_details_gin"),
                "read": ("public", "hub_fts_details"),
                "hub_fk": "detail_id",
                "engine": "fts",
                "fts_groups": fts_groups,
            },
            spec_name: {
                "index": ("public", "idx_hub_fts_specs_gin"),
                "read": ("public", "hub_fts_specs"),
                "hub_fk": "spec_id",
                "engine": "fts",
                "fts_groups": fts_groups,
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
    adapter = ConfigurablePostgresHubSearch(config=hub_fts_cfg)(ctx_hub, hub_spec)

    __p = await adapter.search_page("alpha")
    hits = __p.hits
    cnt = __p.count
    assert cnt == 2
    assert {h.id for h in hits} == {lid1, lid3}

    __p = await adapter.search_page(
        "gamma", filters={"$fields": {"spec_id": str(s1)}}
    )
    hits2 = __p.hits
    cnt2 = __p.count
    assert cnt2 == 2
    assert {h.id for h in hits2} == {lid1, lid2}

    __p = await adapter.search_page("")
    fts_browse = __p.hits
    fts_cnt = __p.count
    assert fts_cnt == 3
    assert {h.id for h in fts_browse} == {lid1, lid2, lid3}


@pytest.mark.asyncio
async def test_postgres_hub_pgroonga_combine_or_vs_and(
    pg_client: PostgresClient,
) -> None:
    """``combine_strategy`` OR includes a link if any leg matches; AND requires every leg."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE hub_and_detail (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_and_spec (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_and_link (
            id uuid PRIMARY KEY,
            detail_id uuid NOT NULL REFERENCES hub_and_detail (id),
            spec_id uuid NOT NULL REFERENCES hub_and_spec (id),
            quantity int NOT NULL
        );
        """
    )
    await pg_client.execute(
        """
        CREATE INDEX idx_hub_and_d ON hub_and_detail
        USING pgroonga ((ARRAY[name, display_name]));
        CREATE INDEX idx_hub_and_s ON hub_and_spec
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )

    d1, d2 = uuid4(), uuid4()
    s1, s2 = uuid4(), uuid4()
    await pg_client.execute(
        "INSERT INTO hub_and_detail (id, name, display_name) VALUES (%(id)s, 'findme', 'a')",
        {"id": d1},
    )
    await pg_client.execute(
        "INSERT INTO hub_and_detail (id, name, display_name) VALUES (%(id)s, 'findme', 'b')",
        {"id": d2},
    )
    await pg_client.execute(
        "INSERT INTO hub_and_spec (id, name, display_name) VALUES (%(id)s, 'other', 'c')",
        {"id": s1},
    )
    await pg_client.execute(
        "INSERT INTO hub_and_spec (id, name, display_name) VALUES (%(id)s, 'findme', 'd')",
        {"id": s2},
    )
    lid_or, lid_and = uuid4(), uuid4()
    await pg_client.execute(
        (
            "INSERT INTO hub_and_link (id, detail_id, spec_id, quantity) VALUES "
            "(%(a)s, %(d1)s, %(s1)s, 1), (%(b)s, %(d2)s, %(s2)s, 3)"
        ),
        {"a": lid_or, "b": lid_and, "d1": d1, "d2": d2, "s1": s1, "s2": s2},
    )

    det_name = "dleg"
    spec_name = "sleg"
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
        name="hub_and_search",
        model_type=LinkModel,
        members=(detail_txt, spec_txt),
    )

    base_members: PostgresHubSearchConfig = {
        "hub": ("public", "hub_and_link"),
        "members": {
            det_name: {
                "index": ("public", "idx_hub_and_d"),
                "read": ("public", "hub_and_detail"),
                "hub_fk": "detail_id",
            },
            spec_name: {
                "index": ("public", "idx_hub_and_s"),
                "read": ("public", "hub_and_spec"),
                "hub_fk": "spec_id",
            },
        },
        "score_merge": "max",
    }

    introspector = PostgresIntrospector(client=pg_client)
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: introspector,
            }
        )
    )

    adapter_or = ConfigurablePostgresHubSearch(
        config={**base_members, "combine_strategy": "or"}
    )(ctx, hub_spec)
    __p = await adapter_or.search_page("findme")
    hits_or = __p.hits
    n_or = __p.count
    assert n_or == 2
    assert {h.id for h in hits_or} == {lid_or, lid_and}

    adapter_and = ConfigurablePostgresHubSearch(
        config={**base_members, "combine_strategy": "and"}
    )(ctx, hub_spec)
    __p = await adapter_and.search_page("findme")
    hits_and = __p.hits
    n_and = __p.count
    assert n_and == 1
    assert hits_and[0].id == lid_and


@pytest.mark.asyncio
async def test_postgres_hub_mixed_pgroonga_and_fts_legs(
    pg_client: PostgresClient,
) -> None:
    """One PGroonga leg and one FTS leg on the same link hub."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE hub_mix_detail (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_mix_spec (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_mix_link (
            id uuid PRIMARY KEY,
            detail_id uuid NOT NULL REFERENCES hub_mix_detail (id),
            spec_id uuid NOT NULL REFERENCES hub_mix_spec (id),
            quantity int NOT NULL
        );
        """
    )
    await pg_client.execute(
        """
        CREATE INDEX idx_hub_mix_d_pg ON hub_mix_detail
        USING pgroonga ((ARRAY[name, display_name]));
        CREATE INDEX idx_hub_mix_s_fts ON hub_mix_spec
        USING gin (to_tsvector('english', coalesce(name, '') || ' ' || coalesce(display_name, '')));
        """
    )

    d1, d2 = uuid4(), uuid4()
    s1, s2 = uuid4(), uuid4()
    await pg_client.execute(
        "INSERT INTO hub_mix_detail (id, name, display_name) VALUES (%(id)s, 'mixed alpha', 'a')",
        {"id": d1},
    )
    await pg_client.execute(
        "INSERT INTO hub_mix_detail (id, name, display_name) VALUES (%(id)s, 'mixed beta', 'b')",
        {"id": d2},
    )
    await pg_client.execute(
        "INSERT INTO hub_mix_spec (id, name, display_name) VALUES (%(id)s, 'mixed gamma', 'c')",
        {"id": s1},
    )
    await pg_client.execute(
        "INSERT INTO hub_mix_spec (id, name, display_name) VALUES (%(id)s, 'mixed delta', 'd')",
        {"id": s2},
    )
    lid1, lid2, lid3 = uuid4(), uuid4(), uuid4()
    await pg_client.execute(
        (
            "INSERT INTO hub_mix_link (id, detail_id, spec_id, quantity) VALUES "
            "(%(a)s, %(d1)s, %(s1)s, 1), (%(b)s, %(d2)s, %(s1)s, 2), (%(c)s, %(d1)s, %(s2)s, 3)"
        ),
        {"a": lid1, "b": lid2, "c": lid3, "d1": d1, "d2": d2, "s1": s1, "s2": s2},
    )

    det_name = "mix_d"
    spec_name = "mix_s"
    fts_groups = {"A": ("name",), "B": ("display_name",)}
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
        name="hub_mix_search",
        model_type=LinkModel,
        members=(detail_txt, spec_txt),
    )

    hub_mix_cfg: PostgresHubSearchConfig = {
        "hub": ("public", "hub_mix_link"),
        "members": {
            det_name: {
                "index": ("public", "idx_hub_mix_d_pg"),
                "read": ("public", "hub_mix_detail"),
                "hub_fk": "detail_id",
                "engine": "pgroonga",
            },
            spec_name: {
                "index": ("public", "idx_hub_mix_s_fts"),
                "read": ("public", "hub_mix_spec"),
                "hub_fk": "spec_id",
                "engine": "fts",
                "fts_groups": fts_groups,
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
    adapter = ConfigurablePostgresHubSearch(config=hub_mix_cfg)(ctx_hub, hub_spec)

    __p = await adapter.search_page("mixed")
    cnt = __p.count
    assert cnt == 3

    __p = await adapter.search_page("alpha")
    hits_alpha = __p.hits
    n_alpha = __p.count
    assert n_alpha == 2
    assert {h.id for h in hits_alpha} == {lid1, lid3}

    __p = await adapter.search_page(
        "gamma", filters={"$fields": {"spec_id": str(s1)}}
    )
    hits_gamma = __p.hits
    n_gamma = __p.count
    assert n_gamma == 2
    assert {h.id for h in hits_gamma} == {lid1, lid2}


@pytest.mark.asyncio
async def test_postgres_hub_pgroonga_multi_hub_fk_one_heap(
    pg_client: PostgresClient,
) -> None:
    """Two hub FK columns reference the same heap (OR linkage); second leg uses another heap."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    await pg_client.execute(
        """
        CREATE TABLE hub_mfk_parties (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_mfk_labels (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE hub_mfk_contracts (
            id uuid PRIMARY KEY,
            party_a_id uuid NOT NULL REFERENCES hub_mfk_parties (id),
            party_b_id uuid NOT NULL REFERENCES hub_mfk_parties (id),
            label_id uuid NOT NULL REFERENCES hub_mfk_labels (id)
        );
        """
    )
    await pg_client.execute(
        """
        CREATE INDEX idx_hub_mfk_parties_pg ON hub_mfk_parties
        USING pgroonga ((ARRAY[name, display_name]));
        CREATE INDEX idx_hub_mfk_labels_pg ON hub_mfk_labels
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )

    pa, pb, pc = uuid4(), uuid4(), uuid4()
    lbl = uuid4()
    await pg_client.execute(
        "INSERT INTO hub_mfk_parties (id, name, display_name) VALUES "
        "(%(a)s, 'north party', 'Alpha North'), (%(b)s, 'south party', 'Beta South'), "
        "(%(c)s, 'east party', 'Gamma East')",
        {"a": pa, "b": pb, "c": pc},
    )
    await pg_client.execute(
        "INSERT INTO hub_mfk_labels (id, name, display_name) VALUES "
        "(%(id)s, 'priority label', 'Label Z')",
        {"id": lbl},
    )
    c1, c2 = uuid4(), uuid4()
    await pg_client.execute(
        (
            "INSERT INTO hub_mfk_contracts "
            "(id, party_a_id, party_b_id, label_id) VALUES "
            "(%(c1)s, %(pa)s, %(pb)s, %(lbl)s), (%(c2)s, %(pb)s, %(pc)s, %(lbl)s)"
        ),
        {"c1": c1, "c2": c2, "pa": pa, "pb": pb, "pc": pc, "lbl": lbl},
    )

    party_leg = "parties_mfk"
    label_leg = "labels_mfk"
    party_txt = SearchSpec(
        name=party_leg,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    label_txt = SearchSpec(
        name=label_leg,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name="hub_mfk_contracts_search",
        model_type=ContractMultiFkModel,
        members=(party_txt, label_txt),
    )

    hub_pg: PostgresHubSearchConfig = {
        "hub": ("public", "hub_mfk_contracts"),
        "members": {
            party_leg: {
                "index": ("public", "idx_hub_mfk_parties_pg"),
                "read": ("public", "hub_mfk_parties"),
                "hub_fk": ["party_a_id", "party_b_id"],
            },
            label_leg: {
                "index": ("public", "idx_hub_mfk_labels_pg"),
                "read": ("public", "hub_mfk_labels"),
                "hub_fk": "label_id",
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

    __p = await adapter.search_page("Alpha")
    hits_alpha = __p.hits
    n_alpha = __p.count
    assert n_alpha == 1
    assert hits_alpha[0].id == c1

    __p = await adapter.search_page("Gamma")
    hits_gamma = __p.hits
    n_gamma = __p.count
    assert n_gamma == 1
    assert hits_gamma[0].id == c2

    __p = await adapter.search_page("Beta")
    hits_beta = __p.hits
    n_beta = __p.count
    assert n_beta == 2
    assert {h.id for h in hits_beta} == {c1, c2}

    __p = await adapter.search_page(
        "East",
        options={"member_weights": {party_leg: 1.0, label_leg: 0.0}},
    )
    only_party = __p.hits
    n_po = __p.count
    assert n_po == 1
    assert only_party[0].id == c2

    __p = await adapter.search_page("")
    browse = __p.hits
    n_all = __p.count
    assert n_all == 2
    assert {h.id for h in browse} == {c1, c2}


class _SameHeapHubRow(BaseModel):
    id: UUID
    name: str
    display_name: str


@pytest.mark.asyncio
async def test_postgres_hub_same_heap_as_hub_single_leg(
    pg_client: PostgresClient,
) -> None:
    """Hub leg on the same table as the hub uses the hf-only path (no heap self-join)."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        """
        CREATE TABLE hub_same_heap (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_hub_same_heap_pg ON hub_same_heap
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    a, b = uuid4(), uuid4()
    await pg_client.execute(
        (
            "INSERT INTO hub_same_heap (id, name, display_name) VALUES "
            "(%(a)s, %(na)s, %(da)s), (%(b)s, %(nb)s, %(db)s)"
        ),
        {
            "a": a,
            "na": "match one",
            "da": "First",
            "b": b,
            "nb": "other",
            "db": "Second",
        },
    )

    leg_name = "doc_leg"
    doc_leg = SearchSpec(
        name=leg_name,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name="hub_same_heap_search",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    hub_pg: PostgresHubSearchConfig = {
        "hub": ("public", "hub_same_heap"),
        "members": {
            leg_name: {
                "index": ("public", "idx_hub_same_heap_pg"),
                "read": ("public", "hub_same_heap"),
                "hub_fk": "id",
                "same_heap_as_hub": True,
            },
        },
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

    __p = await adapter.search_page("match")
    assert __p.count == 1
    assert __p.hits[0].id == a

    __p = await adapter.search_page("")
    assert __p.count == 2
    assert {h.id for h in __p.hits} == {a, b}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_pgroonga_v2_empty_query_filter_only_paths(
    pg_client: PostgresClient,
) -> None:
    """No full-text terms: projection scan, optional count, limit/offset, ``return_fields``."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    heap = f"es_heap_{suffix}"
    proj = f"es_proj_{suffix}"
    idx = f"es_idx_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {heap} (
            id uuid PRIMARY KEY,
            doc_title text NOT NULL,
            doc_body text NOT NULL
        );
        CREATE VIEW {proj} AS
        SELECT id, doc_title AS title, doc_body AS content FROM {heap};
        CREATE INDEX {idx}
        ON {heap} USING pgroonga ((ARRAY[doc_title, doc_body]));
        """
    )

    d1 = uuid4()
    d2 = uuid4()
    await pg_client.execute(
        f"INSERT INTO {heap} (id, doc_title, doc_body) VALUES (%(id)s, 'Apple', 'red')",
        {"id": d1},
    )
    await pg_client.execute(
        f"INSERT INTO {heap} (id, doc_title, doc_body) VALUES (%(id)s, 'Banana', 'yellow')",
        {"id": d2},
    )

    introspector = PostgresIntrospector(client=pg_client)
    spec = SearchSpec(
        name=f"es_{suffix}",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        source_qname=PostgresQualifiedName(schema="public", name=proj),
        index_qname=PostgresQualifiedName(schema="public", name=idx),
        index_heap_qname=PostgresQualifiedName(schema="public", name=heap),
        client=pg_client,
        model_type=SearchableModel,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
        index_field_map={"title": "doc_title", "content": "doc_body"},
    )

    z = await adapter.search_page(
        "",
        filters={"$fields": {"title": "Nope"}},
    )
    assert z.count == 0
    assert z.hits == []

    page = await adapter.project_search_page(
        ("title", "id"),
        "",
        filters={"$fields": {"title": "Apple"}},
        pagination={"limit": 5, "offset": 0},
        sorts={"title": "asc"},
    )
    assert page.count == 1
    assert page.hits[0] == {"title": "Apple", "id": d1}

    class Row(BaseModel):
        id: UUID
        title: str

    typed = await adapter.select_search(
        Row,
        "",
        sorts={"title": "desc"},
    )
    assert [r.title for r in typed.hits] == ["Banana", "Apple"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_pgroonga_v2_nonempty_query_count_zero_short_circuit(
    pg_client: PostgresClient,
) -> None:
    """Ranked PGroonga path: ``return_count`` with zero hits skips the data query."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    heap = f"zq_heap_{suffix}"
    proj = f"zq_proj_{suffix}"
    idx = f"zq_idx_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {heap} (
            id uuid PRIMARY KEY,
            doc_title text NOT NULL,
            doc_body text NOT NULL
        );
        CREATE VIEW {proj} AS
        SELECT id, doc_title AS title, doc_body AS content FROM {heap};
        CREATE INDEX {idx}
        ON {heap} USING pgroonga ((ARRAY[doc_title, doc_body]));
        """
    )
    await pg_client.execute(
        f"INSERT INTO {heap} (id, doc_title, doc_body) VALUES (%(id)s, 'only', 'row');",
        {"id": uuid4()},
    )

    introspector = PostgresIntrospector(client=pg_client)
    spec = SearchSpec(
        name=f"zq_{suffix}",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        source_qname=PostgresQualifiedName(schema="public", name=proj),
        index_qname=PostgresQualifiedName(schema="public", name=idx),
        index_heap_qname=PostgresQualifiedName(schema="public", name=heap),
        client=pg_client,
        model_type=SearchableModel,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
        index_field_map={"title": "doc_title", "content": "doc_body"},
    )

    empty = await adapter.search_page("xyznonmatch12345")
    assert empty.count == 0
    assert empty.hits == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_pgroonga_v2_ranked_search_uses_score_v1(
    pg_client: PostgresClient,
) -> None:
    """``pgroonga_score_version='v1'`` still returns ranked hits."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    heap = f"v1_heap_{suffix}"
    proj = f"v1_proj_{suffix}"
    idx = f"v1_idx_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {heap} (
            id uuid PRIMARY KEY,
            doc_title text NOT NULL,
            doc_body text NOT NULL
        );
        CREATE VIEW {proj} AS
        SELECT id, doc_title AS title, doc_body AS content FROM {heap};
        CREATE INDEX {idx}
        ON {heap} USING pgroonga ((ARRAY[doc_title, doc_body]));
        """
    )
    await pg_client.execute(
        f"INSERT INTO {heap} (id, doc_title, doc_body) VALUES (%(id)s, 'alpha', 'beta gamma')",
        {"id": uuid4()},
    )

    introspector = PostgresIntrospector(client=pg_client)
    spec = SearchSpec(
        name=f"v1_{suffix}",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        source_qname=PostgresQualifiedName(schema="public", name=proj),
        index_qname=PostgresQualifiedName(schema="public", name=idx),
        index_heap_qname=PostgresQualifiedName(schema="public", name=heap),
        client=pg_client,
        model_type=SearchableModel,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
        index_field_map={"title": "doc_title", "content": "doc_body"},
        pgroonga_score_version="v1",
    )

    page = await adapter.search_page("gamma")
    assert page.count == 1
    assert page.hits[0].title == "alpha"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_pgroonga_v2_search_with_cursor_filter_only(
    pg_client: PostgresClient,
) -> None:
    """Keyset pagination on the projection when the full-text query is empty."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    heap = f"cur_heap_{suffix}"
    proj = f"cur_proj_{suffix}"
    idx = f"cur_idx_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {heap} (
            id uuid PRIMARY KEY,
            doc_title text NOT NULL,
            doc_body text NOT NULL
        );
        CREATE VIEW {proj} AS
        SELECT id, doc_title AS title, doc_body AS content FROM {heap};
        CREATE INDEX {idx}
        ON {heap} USING pgroonga ((ARRAY[doc_title, doc_body]));
        """
    )
    for title in ("a", "b", "c"):
        await pg_client.execute(
            f"INSERT INTO {heap} (id, doc_title, doc_body) VALUES (%(id)s, %(t)s, 'x')",
            {"id": uuid4(), "t": title},
        )

    introspector = PostgresIntrospector(client=pg_client)
    spec = SearchSpec(
        name=f"cur_{suffix}",
        model_type=SearchableModel,
        fields=["title", "content"],
    )
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        source_qname=PostgresQualifiedName(schema="public", name=proj),
        index_qname=PostgresQualifiedName(schema="public", name=idx),
        index_heap_qname=PostgresQualifiedName(schema="public", name=heap),
        client=pg_client,
        model_type=SearchableModel,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
        index_field_map={"title": "doc_title", "content": "doc_body"},
    )

    with pytest.raises(CoreError, match="at most one"):
        await adapter.search_cursor("", cursor={"after": "x", "before": "y"})

    with pytest.raises(CoreError, match="positive"):
        await adapter.search_cursor("", cursor={"limit": 0})

    p0 = await adapter.project_search_cursor(
        ["title"],
        "",
        sorts={"title": "asc"},
        cursor={"limit": 2},
    )
    assert len(p0.hits) == 2
    assert set(p0.hits[0].keys()) == {"title"}

    p1 = await adapter.project_search_cursor(
        ["title", "content", "id"],
        "",
        sorts={"title": "asc"},
        cursor={"limit": 2},
    )
    assert len(p1.hits) == 2
    assert p1.has_more is True
    assert p1.next_cursor is not None

    p2 = await adapter.project_search_cursor(
        ["title", "content", "id"],
        "",
        sorts={"title": "asc"},
        cursor={"limit": 2, "after": p1.next_cursor},
    )
    assert len(p2.hits) >= 1

    class Hit(BaseModel):
        id: UUID
        title: str

    p3 = await adapter.select_search_cursor(
        Hit,
        "",
        sorts={"title": "asc"},
        cursor={"limit": 10},
    )
    assert len(p3.hits) == 3
    assert isinstance(p3.hits[0], Hit)

    r1 = await adapter.project_search_cursor(
        ["title", "content", "id"],
        "x",
        sorts={"title": "asc"},
        cursor={"limit": 2},
    )
    assert len(r1.hits) >= 1
    assert set(r1.hits[0].keys()) == {"title", "content", "id"}
    if r1.has_more and r1.next_cursor:
        r2 = await adapter.project_search_cursor(
            ["title", "content", "id"],
            "x",
            sorts={"title": "asc"},
            cursor={"limit": 2, "after": r1.next_cursor},
        )
        assert len(r2.hits) >= 1
        assert set(r2.hits[0].keys()) == {"title", "content", "id"}


class _MqBody(BaseModel):
    body: str


class MqLinkModel(BaseModel):
    id: UUID
    body_id: UUID


@pytest.mark.asyncio
async def test_postgres_hub_fts_leg_multi_query_phrase_combine(
    pg_client: PostgresClient,
) -> None:
    """FTS hub leg with multiple terms: conjunction vs disjunction tsquery."""
    suffix = uuid4().hex[:8]
    body_t = f"hub_mq_body_{suffix}"
    link_t = f"hub_mq_link_{suffix}"
    idx = f"idx_hub_mq_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {body_t} (
            id uuid PRIMARY KEY,
            body text NOT NULL
        );
        CREATE TABLE {link_t} (
            id uuid PRIMARY KEY,
            body_id uuid NOT NULL REFERENCES {body_t} (id)
        );
        CREATE INDEX {idx} ON {body_t}
        USING gin (to_tsvector('english', coalesce(body, '')));
        """
    )

    b1, b2, b3 = uuid4(), uuid4(), uuid4()
    l1, l2, l3 = uuid4(), uuid4(), uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO {body_t} (id, body) VALUES
        (%(b1)s, 'alpha beta gamma'),
        (%(b2)s, 'alpha only here'),
        (%(b3)s, 'beta standalone')
        """,
        {"b1": b1, "b2": b2, "b3": b3},
    )
    await pg_client.execute(
        f"""
        INSERT INTO {link_t} (id, body_id) VALUES
        (%(l1)s, %(b1)s), (%(l2)s, %(b2)s), (%(l3)s, %(b3)s)
        """,
        {"l1": l1, "l2": l2, "l3": l3, "b1": b1, "b2": b2, "b3": b3},
    )

    leg_n = "mq_body"
    body_spec = SearchSpec(
        name=leg_n,
        model_type=_MqBody,
        fields=["body"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_mq_{suffix}",
        model_type=MqLinkModel,
        members=(body_spec,),
    )
    fts_groups = {"A": ("body",)}
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", link_t),
        "members": {
            leg_n: {
                "index": ("public", idx),
                "read": ("public", body_t),
                "hub_fk": "body_id",
                "engine": "fts",
                "fts_groups": fts_groups,
            },
        },
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    page_all = await adapter.search_page(
        ["alpha", "beta"],
        options={"phrase_combine": "all"},
    )
    assert page_all.count == 1
    assert page_all.hits[0].id == l1

    page_any = await adapter.search_page(
        ["alpha", "beta"],
        options={"phrase_combine": "any"},
    )
    assert page_any.count == 3


@pytest.mark.asyncio
async def test_postgres_hub_combine_and_with_score_merge_sum(
    pg_client: PostgresClient,
) -> None:
    """``combine: and`` requires every leg to match; ``score_merge: sum`` merges scores."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    dt = f"hub_sum_d_{suffix}"
    st = f"hub_sum_s_{suffix}"
    lt = f"hub_sum_l_{suffix}"
    idx_d = f"idx_sum_d_{suffix}"
    idx_s = f"idx_sum_s_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {dt} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE {st} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE TABLE {lt} (
            id uuid PRIMARY KEY,
            detail_id uuid NOT NULL REFERENCES {dt} (id),
            spec_id uuid NOT NULL REFERENCES {st} (id),
            quantity int NOT NULL
        );
        CREATE INDEX {idx_d} ON {dt}
        USING pgroonga ((ARRAY[name, display_name]));
        CREATE INDEX {idx_s} ON {st}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )

    d1, s1 = uuid4(), uuid4()
    link_id = uuid4()
    await pg_client.execute(
        f"INSERT INTO {dt} (id, name, display_name) VALUES (%(id)s, 'unique detail', 'D')",
        {"id": d1},
    )
    await pg_client.execute(
        f"INSERT INTO {st} (id, name, display_name) VALUES (%(id)s, 'unique spec', 'S')",
        {"id": s1},
    )
    await pg_client.execute(
        f"""
        INSERT INTO {lt} (id, detail_id, spec_id, quantity)
        VALUES (%(lid)s, %(d)s, %(s)s, 1)
        """,
        {"lid": link_id, "d": d1, "s": s1},
    )

    det_name = f"sum_d_{suffix}"
    spec_name = f"sum_s_{suffix}"
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
        name=f"hub_sum_{suffix}",
        model_type=LinkModel,
        members=(detail_txt, spec_txt),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", lt),
        "members": {
            det_name: {
                "index": ("public", idx_d),
                "read": ("public", dt),
                "hub_fk": "detail_id",
                "engine": "pgroonga",
            },
            spec_name: {
                "index": ("public", idx_s),
                "read": ("public", st),
                "hub_fk": "spec_id",
                "engine": "pgroonga",
            },
        },
        "combine_strategy": "and",
        "merge_strategy": "sum",
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    page = await adapter.search_page("unique")
    assert page.count == 1
    assert page.hits[0].id == link_id

    empty = await adapter.search_page("onlydetailnomatch")
    assert empty.count == 0


class HubHitId(BaseModel):
    id: UUID


@pytest.mark.asyncio
async def test_postgres_hub_return_count_zero_and_projections(
    pg_client: PostgresClient,
) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    ht = f"hub_proj_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_pg ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    u = uuid4()
    await pg_client.execute(
        f"INSERT INTO {ht} (id, name, display_name) VALUES (%(id)s, 'solo', 'S')",
        {"id": u},
    )

    leg_n = f"leg_{suffix}"
    doc_leg = SearchSpec(
        name=leg_n,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_proj_spec_{suffix}",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", ht),
        "members": {
            leg_n: {
                "index": ("public", f"idx_{suffix}_pg"),
                "read": ("public", ht),
                "hub_fk": "id",
                "same_heap_as_hub": True,
            },
        },
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    impossible: QueryFilterExpression = {"$fields": {"name": "nope"}}
    z = await adapter.search_page("solo", filters=impossible)
    assert isinstance(z, Page)
    assert z.count == 0
    assert z.hits == []

    rf = await adapter.project_search(
        ["id", "name"],
        "solo",
    )
    assert not isinstance(rf, Page)
    assert rf.hits[0] == {"id": u, "name": "solo"}

    rt = await adapter.select_search(HubHitId, "solo")
    assert isinstance(rt, CountlessPage)
    assert isinstance(rt.hits[0], HubHitId)


@pytest.mark.asyncio
async def test_postgres_hub_browse_empty_query_with_sorts(
    pg_client: PostgresClient,
) -> None:
    """No search terms: hub scan with explicit ``sorts`` (no leg CTEs)."""
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    ht = f"hub_br_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_br ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    a, b = uuid4(), uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO {ht} (id, name, display_name) VALUES
        (%(a)s, 'b', 'B'), (%(b)s, 'a', 'A')
        """,
        {"a": a, "b": b},
    )

    leg_n = f"br_{suffix}"
    doc_leg = SearchSpec(
        name=leg_n,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_br_spec_{suffix}",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", ht),
        "members": {
            leg_n: {
                "index": ("public", f"idx_{suffix}_br"),
                "read": ("public", ht),
                "hub_fk": "id",
                "same_heap_as_hub": True,
            },
        },
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    page = await adapter.search_page("", sorts={"name": "asc"})
    assert page.count == 2
    assert [h.name for h in page.hits] == ["a", "b"]


@pytest.mark.asyncio
async def test_postgres_hub_search_with_cursor(
    pg_client: PostgresClient,
) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    ht = f"hub_cur_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_cur ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    id_lo = uuid4()
    id_hi = uuid4()
    await pg_client.execute(
        (
            f"INSERT INTO {ht} (id, name, display_name) VALUES "
            "(%(a)s, %(na)s, 'A'), (%(b)s, %(nb)s, 'B')"
        ),
        {"a": id_lo, "na": "alpha", "b": id_hi, "nb": "beta"},
    )

    leg_n = f"cur_{suffix}"
    doc_leg = SearchSpec(
        name=leg_n,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_cur_spec_{suffix}",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", ht),
        "members": {
            leg_n: {
                "index": ("public", f"idx_{suffix}_cur"),
                "read": ("public", ht),
                "hub_fk": "id",
                "same_heap_as_hub": True,
            },
        },
    }

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    with pytest.raises(CoreError, match="at most one"):
        await adapter.search_cursor(
            "",
            cursor={"after": "x", "before": "y"},
        )

    with pytest.raises(CoreError, match="positive"):
        await adapter.search_cursor("", cursor={"limit": 0})

    p0 = await adapter.project_search_cursor(
        ["name"],
        "",
        sorts={"name": "asc"},
        cursor={"limit": 1},
    )
    assert len(p0.hits) == 1
    assert set(p0.hits[0].keys()) == {"name"}

    p1: CursorPage = await adapter.project_search_cursor(
        ["name", "display_name", "id"],
        "",
        sorts={"name": "asc"},
        cursor={"limit": 1},
    )
    assert len(p1.hits) == 1
    assert p1.has_more is True
    assert p1.next_cursor is not None
    assert p1.hits[0]["name"] == "alpha"

    p2 = await adapter.project_search_cursor(
        ["name", "display_name", "id"],
        "",
        sorts={"name": "asc"},
        cursor={"limit": 1, "after": p1.next_cursor},
    )
    assert len(p2.hits) == 1
    assert p2.hits[0]["name"] == "beta"

    r1 = await adapter.project_search_cursor(
        ["id", "name", "display_name"],
        "alpha",
        cursor={"limit": 5},
    )
    assert len(r1.hits) == 1
    assert r1.hits[0]["name"] == "alpha"
    assert set(r1.hits[0].keys()) == {"id", "name", "display_name"}


@pytest.mark.asyncio
async def test_postgres_hub_search_with_cursor_ranked_id_desc_chains(
    pg_client: PostgresClient,
) -> None:
    """Regression: ranked keyset must honor ``id`` direction in ``sorts`` (tied scores)."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    ht = f"hub_cur_id_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_idcur ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    row_ids = [uuid4() for _ in range(12)]
    for uid in row_ids:
        await pg_client.execute(
            f"INSERT INTO {ht} (id, name, display_name) VALUES (%(id)s, 'token', 'x')",
            {"id": uid},
        )

    leg_n = f"idcur_{suffix}"
    doc_leg = SearchSpec(
        name=leg_n,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_idcur_{suffix}",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", ht),
        "members": {
            leg_n: {
                "index": ("public", f"idx_{suffix}_idcur"),
                "read": ("public", ht),
                "hub_fk": "id",
                "same_heap_as_hub": True,
            },
        },
    }
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    collected: list[Any] = []
    next_c: str | None = None
    for _ in range(10):
        cur: dict[str, Any] = {"limit": 5}
        if next_c is not None:
            cur["after"] = next_c
        page = await adapter.project_search_cursor(
            ["id", "name", "display_name"],
            "token",
            sorts={"id": "desc"},
            cursor=cur,
        )
        assert len(page.hits) > 0
        collected.extend(h["id"] for h in page.hits)
        if not page.has_more:
            break
        assert page.next_cursor is not None
        next_c = page.next_cursor

    assert len(collected) == 12
    assert len({str(x) for x in collected}) == 12


@pytest.mark.asyncio
async def test_postgres_hub_search_with_cursor_browse_no_sorts(
    pg_client: PostgresClient,
) -> None:
    """Empty query + no sorts: stable browse uses first read field + id (see hub adapter)."""

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    suffix = uuid4().hex[:8]
    ht = f"hub_browse_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {ht} (
            id uuid PRIMARY KEY,
            name text NOT NULL,
            display_name text NOT NULL
        );
        CREATE INDEX idx_{suffix}_br ON {ht}
        USING pgroonga ((ARRAY[name, display_name]));
        """
    )
    uuids = [uuid4() for _ in range(7)]
    for i, uid in enumerate(uuids):
        await pg_client.execute(
            f"INSERT INTO {ht} (id, name, display_name) VALUES (%(id)s, %(n)s, 'tie')",
            {"id": uid, "n": f"n{i}"},
        )

    leg_n = f"br_{suffix}"
    doc_leg = SearchSpec(
        name=leg_n,
        model_type=_HubLegTxt,
        fields=["name", "display_name"],
    )
    hub_spec = HubSearchSpec(
        name=f"hub_browse_{suffix}",
        model_type=_SameHeapHubRow,
        members=(doc_leg,),
    )
    hub_cfg: PostgresHubSearchConfig = {
        "hub": ("public", ht),
        "members": {
            leg_n: {
                "index": ("public", f"idx_{suffix}_br"),
                "read": ("public", ht),
                "hub_fk": "id",
                "same_heap_as_hub": True,
            },
        },
    }
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    adapter = ConfigurablePostgresHubSearch(config=hub_cfg)(ctx, hub_spec)

    collected: list[Any] = []
    next_c: str | None = None
    for _ in range(10):
        cur: dict[str, Any] = {"limit": 3}
        if next_c is not None:
            cur["after"] = next_c
        page = await adapter.search_cursor("", cursor=cur)
        assert len(page.hits) > 0
        collected.extend(h.id for h in page.hits)
        if not page.has_more:
            break
        assert page.next_cursor is not None
        next_c = page.next_cursor

    assert len(collected) == 7
    assert len(set(collected)) == 7

    off = await adapter.search("", pagination={"limit": 100, "offset": 0})
    assert [r.id for r in off.hits] == collected
