# forze_kits

Pre-built wiring above Forze contracts: domain field kits, aggregate registries and facades,
integration flows, local port adapters, and runtime ergonomics.

## Layout

```text
forze_kits/
  domain/           # field/entity shape kits (mixins, mapping steps, …)
  aggregates/       # document, search, storage, authn (registry + facade)
  integration/      # outbox (notify planned)
  adapters/         # secrets (local SecretsPort backends)
  runtime/          # DistributedLockScope, …
```

## Taxonomy

| Kind | Import path |
|------|-------------|
| Domain shape | `forze_kits.domain.*` |
| Aggregate ops | `forze_kits.aggregates.{document,search,storage,authn}` |
| Integration flow | `forze_kits.integrations.outbox` |
| Local port adapter | `forze_kits.adapters.secrets` |
| Runtime ergonomics | `forze_kits.scopes` |

**Not in kits:** `forze_identity`, `forze_postgres`, `forze_vault`, and other full integration planes.

## Dependency rule

`forze` core must not import `forze_kits`. Kits import `forze.application` and `forze.domain`.

## Adding a new kit

1. Pick the folder (`domain`, `aggregates`, `integration`, `adapters`, `runtime`).
2. Implement under that path with `__init__.py` exports.
3. Do not add new ports here—extend `forze.application.contracts` when a new capability is needed.
4. Add unit tests under `tests/unit/test_forze_kits/`.
5. Document in `pages/docs/reference/kits.md`.
