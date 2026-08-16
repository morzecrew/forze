# Search

Full-text and faceted search through a `SearchFacade`, federated and hub indexes, and rebuilding an index without downtime. Structured filtering is [query DSL](query-dsl.md).

## Search with `SearchFacade`

Drive search through a **`SearchFacade`** built from a `SearchSpec`, the same way:

```python
from forze.application.contracts.search import SearchSpec
from forze_kits.aggregates.search import (
    SearchFacade,
    SearchRequestDTO,
    build_search_registry,
)

project_search = SearchSpec(
    name=ResourceName.PROJECTS,
    model_type=ProjectRead,
    fields=("title", "description"),
    default_weights={"title": 0.7, "description": 0.3},
)
search_registry = build_search_registry(project_search).freeze()


def project_search_facade(ctx) -> SearchFacade[ProjectRead]:
    return SearchFacade(ctx=ctx, registry=search_registry, namespace=project_search.default_namespace)


page = await project_search_facade(ctx).search(
    SearchRequestDTO(query="roadmap", page=1, size=20, filters={"$values": {"status": "active"}})
)
hits, total = page.hits, page.count
```

`ResourceName.PROJECTS` (your spec-name enum) and `ProjectRead` (your read model) are app-defined symbols.

Methods: `search` (typed, offset), `cursor_search` (typed, keyset), `projected_search` / `projected_cursor_search` (raw dict rows). The physical FTS/PGroonga/vector layout belongs in **`PostgresDepsModule.searches`** (or hub/federated maps), never on the spec.

For faceted navigation and result highlighting, declare `facetable_fields` / `highlightable_fields` on the `SearchSpec` and request them per query through search options (`facets=[…]`, `highlight=True`); the page carries `page.facets` and per-hit `page.highlights`, failing closed when a field or backend can't serve them. Per-request options are backend-agnostic — single-index search takes `SearchOptions`, while hub and federated search take `MultiSourceSearchOptions` (adds `member_weights` / `members`).

## Hub and federated search

Use `HubSearchSpec` with `build_hub_search_registry` when one hub entity searches through weighted member legs — it yields the full `SearchFacade` surface. Use `FederatedSearchSpec` with `build_federated_search_registry` to merge independent specs; it registers only the typed `search` and `cursor_search` (no `projected_search` / `projected_cursor_search`). Keep snapshot storage and cursor/keyset behaviour in infrastructure config.

Postgres serves search from the same relation as the documents. Meilisearch is a separate engine with its own index, so the index name and the per-attribute roles live in its deps config — and an attribute the engine was never told about cannot be filtered or sorted on, whatever the spec says:

```python
from forze_meilisearch import (
    MeilisearchClient,
    MeilisearchDepsModule,
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
    meilisearch_lifecycle_step,
)

search_module = MeilisearchDepsModule(
    client=MeilisearchClient(),
    searches={
        ResourceName.PROJECTS: MeilisearchSearchConfig(
            index_uid="projects",
            searchable_attributes=("title", "summary"),
            filterable_attributes=("status", "is_deleted"),
            sortable_attributes=("created_at",),
        ),
    },
    federated_searches={
        ResourceName.EVERYTHING: MeilisearchFederatedSearchConfig(
            members={ResourceName.PROJECTS: 0.7, ResourceName.NOTES: 0.3},
        ),
    },
)
lifecycle = meilisearch_lifecycle_step(url="http://localhost:7700", api_key=meili_key)
```

Federated search across independent indexes is the case with no other home: `members` weights each leg, and the merge policy decides how their scores reconcile into one ranked page.

## Rebuilding a search index

An index is derived state — it can be refilled from the document plane at any time. `rebuild_search_index` is the idempotent, keyset-paged backfill: it upserts live rows and removes soft-deleted ones, so it converges the index toward the documents rather than merely filling it.

```python
from forze_kits.integrations.search import rebuild_search_index

report = await rebuild_search_index(
    ctx.document.query(project_spec),
    ctx.search.command(project_search),
    document=project_spec,
    search=project_search,
)
```

Interrupted sweeps are re-run, not repaired. An `AggregateKit` exposes the same thing as `kit.rebuild_search()`. Run it after an import, after a mapping change, or when an index's provenance is unknown — an exact result wants a source that is not being written.

## Search composition

```python
from forze_kits.aggregates.search import SearchFacade, SearchRequestDTO, build_search_registry

search_registry = build_search_registry(project_search_spec).freeze()

facade = SearchFacade(
    ctx=ctx,
    registry=search_registry,
    namespace=project_search_spec.default_namespace,
)
result = await facade.search(SearchRequestDTO(query="roadmap", page=1, size=20))
```

## Reference

- [Reading data](https://morzecrew.github.io/forze/latest/data-events/reading-data/)
- [Document contracts](https://morzecrew.github.io/forze/latest/reference/contracts/document/)
