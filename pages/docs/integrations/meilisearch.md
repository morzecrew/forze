# Meilisearch Integration

## What this integration provides

`forze_meilisearch` implements `SearchQueryPort`, `SearchCommandPort`, and federated search over Meilisearch indexes using the async `meilisearch-python-sdk`.

Indexed documents are treated as a **read model**: hits are validated as your `SearchSpec.model_type`. Applications call `ctx.search.command(spec).upsert(...)` explicitly; there is no automatic sync from `DocumentCommandPort`.

## When to use it

Use this when Meilisearch is your full-text search backend and you want Forze search contracts (offset pagination, filters, federated multi-index search, optional Redis result snapshots).

Use `RoutedMeilisearchClient` when tenant identity selects Meilisearch URL and API key (row-level isolation can still use `tenant_aware` on search configs).

## Installation

```bash
uv add 'forze[meilisearch]'
```

Integration tests require Docker and `uv sync --extra meilisearch`.

## Runtime wiring

```python
from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
from forze_meilisearch import (
    MeilisearchClient,
    MeilisearchConfig,
    MeilisearchDepsModule,
    meilisearch_lifecycle_step,
)

client = MeilisearchClient()
module = MeilisearchDepsModule(
    client=client,
    searches={
        "articles": {
            "index_uid": "articles",
            "filterable_fields": ["tenant_id", "status"],
            "sortable_fields": ["created_at"],
        },
    },
    federated_searches={
        "catalog": {
            "merge": "federation",
            "members": {
                "products": {"index_uid": "products"},
                "brands": {"index_uid": "brands"},
            },
        },
    },
)

runtime = ExecutionRuntime(
    deps=DepsPlan.from_modules(module),
    lifecycle=LifecyclePlan.from_steps(
        meilisearch_lifecycle_step(
            url="http://localhost:7700",
            api_key="masterKey",
            config=MeilisearchConfig(timeout=30.0),
        )
    ),
)
```

### Routed lifecycle

```python
from forze_meilisearch import RoutedMeilisearchClient, routed_meilisearch_lifecycle_step

routed = RoutedMeilisearchClient(
    secrets=secrets_port,
    secret_ref_for_tenant=lambda tid: SecretRef(f"tenants/{tid}/meilisearch"),
    tenant_provider=ctx.inv_ctx.get_tenant,
)
# LifecyclePlan.from_steps(routed_meilisearch_lifecycle_step(client=routed))
```

Per-tenant JSON: `{"url": "https://search.example.com", "api_key": "..."}` (`MeilisearchRoutingCredentials`).

### What gets registered

| Key | Capability |
|-----|------------|
| `MeilisearchClientDepKey` | Async Meilisearch client |
| `SearchQueryDepKey` | Per-route simple search adapters |
| `SearchCommandDepKey` | Index settings and document upsert/delete |
| `FederatedSearchQueryDepKey` | Multi-index federated search (optional) |

## SearchSpec and Meilisearch config

`MeilisearchSearchConfig` (per `SearchSpec` route):

| Field | Purpose |
|-------|---------|
| `index_uid` | Meilisearch index UID (required) |
| `primary_key` | Document primary key field (default: `id`) |
| `field_map` | Logical → Meilisearch attribute names |
| `filterable_fields` / `sortable_fields` | Applied in `ensure_index` |
| `tenant_aware` | Inject tenant filter when enabled |
| `wait_for_tasks` | Wait for Meilisearch tasks after writes (default `True`) |
| `ranking_rules` | Optional index ranking rules |

`MeilisearchFederatedSearchConfig`:

| Field | Purpose |
|-------|---------|
| `members` | Map of member name → `MeilisearchSearchConfig` (≥2) |
| `merge` | `"federation"` (native Meilisearch federation, default) or `"rrf"` (weighted RRF in-process) |
| `rrf_k`, `rrf_per_leg_limit` | RRF tuning when `merge="rrf"` |

Federated members must be `SearchSpec` only (`HubSearchSpec` is rejected).

## Command port

```python
cmd = ctx.search.command(article_spec)
await cmd.ensure_index()
await cmd.upsert(documents)
await cmd.delete(["id-1"])
await cmd.delete_all()
```

`ensure_index` creates or updates index settings (`searchableAttributes`, `filterableAttributes`, `sortableAttributes`).

## Query port

Resolve with `ctx.search.query(spec)`:

- `search`, `search_page`, `select_search`, `project_search` — offset pagination
- `*_cursor` — not implemented (`CoreException.internal`)
- `project_search*` on federated routes — not implemented

Filters support a subset of `QueryFilterExpression` (`$eq`, `$neq`, order comparisons, `$in`, `$nin`, `$null`, `$and`, `$or`, `$not`). `$like`, set relations, and field-to-field compares raise clear errors.

## Federated search

- **`merge="federation"`**: single `multi_search` with per-leg `federationOptions.weight` (from `SearchOptions.member_weights`; legs with weight ≤ 0 are skipped).
- **`merge="rrf"`**: parallel per-leg `search`, merged with `SearchResultSnapshotCoordinator.weighted_rrf_merge_rows`, then caller sorts applied.

Snapshot fingerprints include `extras={"merge": "federation"}` or `extras={"merge": "rrf", "rrf_k": k}` so federation and RRF runs do not collide in Redis.

## Limitations

| Feature | Status |
|---------|--------|
| Hub search | Not supported |
| Cursor pagination | Not implemented |
| Full filter language | Subset only |
| `forze_mock` command adapter | Not bundled (optional follow-up) |

For framework tests or advanced wiring, prefer `from forze_meilisearch.execution.deps import ConfigurableMeilisearchSearch`, `ConfigurableMeilisearchSearchCommand`, and `ConfigurableMeilisearchFederatedSearch` rather than removed `forze_meilisearch.execution.deps.deps` paths.
