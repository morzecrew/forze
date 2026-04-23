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
    __p = await adapter.search("singleton", options={"fuzzy": True}, return_count=True)
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

    assert isinstance(adapter, PostgresPGroongaSearchAdapterV2)

    __p = await adapter.search("python", return_count=True)
    res = __p.hits
    cnt = __p.count
    assert cnt == 3
    assert len(res) == 3

    __p = await adapter.search("hexagonal", return_count=True)
    res2 = __p.hits
    cnt2 = __p.count
    assert cnt2 == 1
    assert len(res2) == 1
    assert res2[0].title == "Forze Framework"

    class TitleOnly(BaseModel):
        title: str

    __p = await adapter.search("python", return_type=TitleOnly, return_count=True)
    as_titles = __p.hits
    n_t = __p.count
    assert n_t == 3
    assert {r.title for r in as_titles} == {d["title"] for d in docs}

    __p = await adapter.search("zznonexistent999", return_count=True)
    none_rows = __p.hits
    n_none = __p.count
    assert n_none == 0
    assert none_rows == []

    await adapter.search("python", options={"fuzzy": True}, return_count=True)

    # Weighted search, pagination, explicit sort, and partial field projection
    __p = await adapter.search(
        "python",
        pagination={"limit": 1, "offset": 0},
        sorts={"title": "asc"},
        options={"weights": {"title": 0.5, "content": 0.5}}, return_count=True)
    page = __p.hits
    total = __p.count
    assert total == 3
    assert len(page) == 1

    __p = await adapter.search(
        "python",
        return_fields=["title"], return_count=True)
    titles_only = __p.hits
    total_t = __p.count
    assert total_t == 3
    assert set(titles_only[0].keys()) == {"title"}

    __p = await adapter.search(["python", "framework"], return_count=True)
    any_two = __p.hits
    n_any = __p.count
    assert n_any == 3
    __p = await adapter.search(
        ["python", "framework"],
        options={"phrase_combine": "all"}, return_count=True)
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

    __p = await adapter.search("python", return_count=True)
    res = __p.hits
    cnt = __p.count
    assert cnt == 2
    assert {r.title for r in res} == {"Forze Framework", "Postgres Guide"}

    __p = await adapter.search(
        "hexagonal",
        sorts={"title": "asc"}, return_count=True)
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
    assert isinstance(adapter, PostgresPGroongaSearchAdapterV2)

    __p = await adapter.search("hello", return_count=True)
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

    __p = await adapter.search("alpha", return_count=True)
    hits = __p.hits
    cnt = __p.count
    assert cnt == 2
    assert {h.id for h in hits} == {lid1, lid3}

    __p = await adapter.search(
        "alpha",
        sorts={"quantity": "asc"}, return_count=True)
    sorted_by_qty = __p.hits
    cnt_sort = __p.count
    assert cnt_sort == 2
    assert [h.quantity for h in sorted_by_qty] == [1, 3]

    __p = await adapter.search(
        "alpha",
        pagination={"limit": 1, "offset": 0}, return_count=True)
    page1 = __p.hits
    cnt_page = __p.count
    assert cnt_page == 2
    assert len(page1) == 1

    class LinkIdQty(BaseModel):
        id: UUID
        quantity: int

    __p = await adapter.search(
        "alpha",
        return_type=LinkIdQty, return_count=True)
    partial = __p.hits
    cnt_partial = __p.count
    assert cnt_partial == 2
    assert {p.id for p in partial} == {lid1, lid3}
    assert all(isinstance(p.quantity, int) for p in partial)

    __p = await adapter.search(
        "alpha",
        return_fields=["id", "quantity"], return_count=True)
    raw_links = __p.hits
    cnt_raw = __p.count
    assert cnt_raw == 2
    assert {r["id"] for r in raw_links} == {lid1, lid3}

    hub_pg_sum: PostgresHubSearchConfig = {**hub_pg, "score_merge": "sum"}
    adapter_sum = ConfigurablePostgresHubSearch(config=hub_pg_sum)(ctx_hub, hub_spec)
    __p = await adapter_sum.search("alpha", return_count=True)
    sum_hits = __p.hits
    sum_cnt = __p.count
    assert sum_cnt == 2
    assert {h.id for h in sum_hits} == {lid1, lid3}

    __p = await adapter.search(
        "alpha",
        options={"member_weights": {det_name: 0.0, spec_name: 0.0}}, return_count=True)
    all_legs_off = __p.hits
    n_off = __p.count
    assert n_off == 3
    assert {h.id for h in all_legs_off} == {lid1, lid2, lid3}

    __p = await adapter.search("no_such_term_xyz", return_count=True)
    _ = __p.hits
    n_no_match = __p.count
    assert n_no_match == 0

    __p = await adapter.search(
        "gamma",
        filters={"$fields": {"spec_id": str(s1)}}, return_count=True)
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
    __p = await resolved.search("delta", return_count=True)
    same = __p.hits
    c3 = __p.count
    assert c3 == 1
    assert same[0].id == lid3

    __p = await adapter.search("", return_count=True)
    browse = __p.hits
    c_browse = __p.count
    assert c_browse == 3
    assert {h.id for h in browse} == {lid1, lid2, lid3}

    __p = await adapter.search("   \t", return_count=True)
    browse_ws = __p.hits
    c_ws = __p.count
    assert c_ws == 3

    __p = await adapter.search(
        "alpha",
        options={"member_weights": {det_name: 0.0, spec_name: 1.0}}, return_count=True)
    zero_detail = __p.hits
    c_z = __p.count
    assert c_z == 0

    __p = await adapter.search(
        "gamma",
        options={"members": [spec_name]}, return_count=True)
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

    __p = await adapter.search("alpha", return_count=True)
    hits = __p.hits
    cnt = __p.count
    assert cnt == 2
    assert {h.id for h in hits} == {lid1, lid3}

    __p = await adapter.search(
        "gamma",
        filters={"$fields": {"spec_id": str(s1)}}, return_count=True)
    hits2 = __p.hits
    cnt2 = __p.count
    assert cnt2 == 2
    assert {h.id for h in hits2} == {lid1, lid2}

    __p = await adapter.search("", return_count=True)
    fts_browse = __p.hits
    fts_cnt = __p.count
    assert fts_cnt == 3
    assert {h.id for h in fts_browse} == {lid1, lid2, lid3}


@pytest.mark.asyncio
async def test_postgres_hub_pgroonga_combine_or_vs_and(pg_client: PostgresClient) -> None:
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
    __p = await adapter_or.search("findme", return_count=True)
    hits_or = __p.hits
    n_or = __p.count
    assert n_or == 2
    assert {h.id for h in hits_or} == {lid_or, lid_and}

    adapter_and = ConfigurablePostgresHubSearch(
        config={**base_members, "combine_strategy": "and"}
    )(ctx, hub_spec)
    __p = await adapter_and.search("findme", return_count=True)
    hits_and = __p.hits
    n_and = __p.count
    assert n_and == 1
    assert hits_and[0].id == lid_and


@pytest.mark.asyncio
async def test_postgres_hub_mixed_pgroonga_and_fts_legs(pg_client: PostgresClient) -> None:
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

    __p = await adapter.search("mixed", return_count=True)
    hits = __p.hits
    cnt = __p.count
    assert cnt == 3

    __p = await adapter.search("alpha", return_count=True)
    hits_alpha = __p.hits
    n_alpha = __p.count
    assert n_alpha == 2
    assert {h.id for h in hits_alpha} == {lid1, lid3}

    __p = await adapter.search(
        "gamma",
        filters={"$fields": {"spec_id": str(s1)}}, return_count=True)
    hits_gamma = __p.hits
    n_gamma = __p.count
    assert n_gamma == 2
    assert {h.id for h in hits_gamma} == {lid1, lid2}


@pytest.mark.asyncio
async def test_postgres_hub_pgroonga_multi_hub_fk_one_heap(pg_client: PostgresClient) -> None:
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

    __p = await adapter.search("Alpha", return_count=True)
    hits_alpha = __p.hits
    n_alpha = __p.count
    assert n_alpha == 1
    assert hits_alpha[0].id == c1

    __p = await adapter.search("Gamma", return_count=True)
    hits_gamma = __p.hits
    n_gamma = __p.count
    assert n_gamma == 1
    assert hits_gamma[0].id == c2

    __p = await adapter.search("Beta", return_count=True)
    hits_beta = __p.hits
    n_beta = __p.count
    assert n_beta == 2
    assert {h.id for h in hits_beta} == {c1, c2}

    __p = await adapter.search(
        "East",
        options={"member_weights": {party_leg: 1.0, label_leg: 0.0}}, return_count=True)
    only_party = __p.hits
    n_po = __p.count
    assert n_po == 1
    assert only_party[0].id == c2

    __p = await adapter.search("", return_count=True)
    browse = __p.hits
    n_all = __p.count
    assert n_all == 2
    assert {h.id for h in browse} == {c1, c2}
