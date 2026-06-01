# forze_kits

Pre-built wiring above Forze contracts: domain field kits, aggregate registries and facades,
integration flows, and runtime port ergonomics.

## Taxonomy

| Area | Module | Use for |
|------|--------|---------|
| Domain shape | `forze_kits.domain.*` | Mixins, field constants, mapping steps, small handlers |
| Aggregate kit | `forze_kits.document`, `search`, `storage`, `authn` | `OperationRegistry` builders, `*KernelOp`, facades |
| Integration flow | `forze_kits.outbox` | Transactional outbox flush, relay, lifecycle (future: `notify`) |
| Runtime ergonomics | `forze_kits.runtime` | Single-port helpers (`DistributedLockScope`, …) |

## Dependency rule

`forze` core must not import `forze_kits`. Kits import `forze.application` and `forze.domain`.

## Adding a new kit

1. Pick the row above (domain vs aggregate vs integration vs runtime).
2. Add a subpackage under `src/forze_kits/` with `__init__.py` exports.
3. Do not add new ports here—extend `forze.application.contracts` if a new capability is needed.
4. Add unit tests under `tests/unit/test_forze_kits/`.
5. Document in `pages/docs/reference/kits.md`.
