# forze_kits

Pre-built wiring above Forze contracts: domain field kits, aggregate registries and facades,
integration flows, local port adapters, and runtime ergonomics.

## Layout

```text
forze_kits/
  domain/           # field/entity shape kits (mixins, mapping steps, …)
  aggregates/       # document, search, storage, stored_file, authn (registry, facade, handlers/)
  mapping/          # Pydantic pipeline mapper factory and steps
  dto/              # shared pagination request/response DTOs
  integrations/     # outbox relay, notify routing
  adapters/         # secrets (local SecretsPort backends)
  scopes/           # DistributedLockScope, …
```

## Taxonomy

| Kind | Import path |
|------|-------------|
| Domain shape | `forze_kits.domain.*` (including `forze_kits.domain.stored_file`) |
| Aggregate ops | `forze_kits.aggregates.{document,search,storage,stored_file,authn}` |
| Default handlers | `forze_kits.aggregates.<name>.handlers` |
| DTO mapping | `forze_kits.mapping` |
| Pagination DTOs | `forze_kits.dto` |
| Integration flow | `forze_kits.integrations.outbox`, `forze_kits.integrations.notify` (stored-file wiring lives in `forze_kits.aggregates.stored_file`) |
| Local port adapter | `forze_kits.adapters.secrets` |
| Runtime ergonomics | `forze_kits.scopes` |

**Not in kits:** `forze_identity`, `forze_postgres`, `forze_vault`, and other full integration planes.

## Dependency rule

`forze` core must not import `forze_kits`. Kits import `forze.application` and `forze.domain`.

## Adding a new kit

1. Pick the folder (`domain`, `aggregates`, `mapping`, `dto`, `integrations`, `adapters`, `scopes`).
2. Implement under that path with `__init__.py` exports.
3. Do not add new ports here—extend `forze.application.contracts` when a new capability is needed.
4. Add unit tests under `tests/unit/test_forze_kits/`.
5. Document in `pages/docs/reference/kits.md`.
