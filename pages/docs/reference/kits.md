---
title: Kits
summary: Pre-built wiring above Forze contracts (registries, facades, domain helpers, integration flows)
---

# Kits (`forze_kits`)

The **`forze_kits`** package ships with the default wheel. It provides canonical wiring above atomic contracts and handlers—without adding new ports. Core **`forze`** must not import **`forze_kits`**; your application imports both.

## Taxonomy

| Area | Module | Use for |
|------|--------|---------|
| Domain shape | `forze_kits.domain.*` | Mixins, field constants, mapping steps, small handlers |
| Aggregate kit | `forze_kits.document`, `search`, `storage`, `authn` | `OperationRegistry` builders, `*KernelOp`, facades |
| Integration flow | `forze_kits.outbox` | Transactional outbox flush, relay, lifecycle (`notify` planned) |
| Secrets (local) | `forze_kits.secrets` | Stdlib `SecretsPort` backends + `SecretsDepsModule` |
| Runtime ergonomics | `forze_kits.runtime` | Single-port helpers (`DistributedLockScope`, …) |

Operation registry mechanics (`.bind()`, `.freeze()`, stage hooks) are documented under [Operation composition](../concepts/operation-composition.md)—that is **execution**, not this package.

## Import map (migration)

| Removed | Use instead |
|---------|-------------|
| `forze_patterns.*` | `forze_kits.domain.*` |
| `forze.application.composition.document` | `forze_kits.document` |
| `forze.application.composition.search` | `forze_kits.search` |
| `forze.application.composition.storage` | `forze_kits.storage` |
| `forze.application.composition.authn` | `forze_kits.authn` |
| `forze.application.composition.outbox` | `forze_kits.outbox` |
| `forze.application.kit` | `forze_kits.runtime` |
| `forze_secrets` | `forze_kits.secrets` |

## Document kit

### `build_document_registry`

Creates an `OperationRegistry` pre-populated with standard document handler factories:

    :::python
    from forze_kits.document import (
        DocumentDTOs,
        build_document_registry,
    )

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)

Use `spec.default_namespace` with `DocumentKernelOp` when you need fully qualified operation keys. Bind transaction routes and freeze before FastAPI attach:

    :::python
    from forze_kits.document import DocumentKernelOp

    write_ops = [
        project_spec.default_namespace.key(op)
        for op in (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE, DocumentKernelOp.KILL)
    ]
    registry = (
        registry.bind(*write_ops)
        .bind_tx()
        .set_route("default")
        .finish(deep=True)
        .freeze()
    )

### `DocumentFacade`

Typed facade exposing document operations as attributes (requires a frozen registry):

    :::python
    from forze_kits.document import DocumentFacade

    facade = DocumentFacade(
        ctx=ctx,
        registry=registry,
        namespace=project_spec.default_namespace,
    )

    project = await facade.create(CreateProjectCmd(title="New"))
    fetched = await facade.get(DocumentIdDTO(id=project.id))

Facade attributes are namespace-aware `facade_op(...)` descriptors. HTTP endpoint specs carry `operation: StrKey` with the fully qualified key (see [FastAPI integration](../integrations/fastapi.md)).

### Custom operations

Register custom handlers on the same registry:

    :::python
    registry = registry.set_handler(
        project_spec.default_namespace.key("archive"),
        lambda ctx: ArchiveProject(doc=ctx.document.command(project_spec)),
        override=True,
    )

Add stages with `.bind(...).bind_outer().before(...)` as needed, then `.freeze()`.

## Search kit

### `build_search_registry`

    :::python
    from forze_kits.search import build_search_registry

    search_registry = build_search_registry(search_spec).freeze()

### `SearchFacade`

    :::python
    from forze_kits.search import SearchFacade

    facade = SearchFacade(
        ctx=ctx,
        registry=search_registry,
        namespace=search_spec.default_namespace,
    )
    result = await facade.search(SearchRequestDTO(query="roadmap", limit=20))

Hub and federated search use `build_hub_search_registry` and `build_federated_search_registry` with the same freeze pattern.

## Storage kit

`build_storage_registry(storage_spec)` registers `upload`, `list`, `download`, and `delete` handlers. `StorageFacade` resolves them through `registry` + `namespace`. Bind tx routes for write operations, then `.freeze()` before `attach_storage_endpoints`.

## Authn kit

`build_authn_registry(authn_spec)` registers `password_login`, `refresh_tokens`, `logout`, and `change_password`. `AuthnFacade` uses the same namespace-aware facade contract. Freeze the registry before `attach_authn_endpoints`.

## Outbox kit

See [Outbox contracts](../core-package/contracts/outbox.md) and [Transactional outbox](../recipes/transactional-outbox.md). Helpers live in `forze_kits.outbox` (`outbox_flush_tx_on_success_factory`, `relay_outbox_to_queue`, `outbox_relay_background_lifecycle_step`).

## Secrets kit (local)

Stdlib-backed implementations of [`SecretsPort`](../core-package/contracts.md) for local development and simple deployments. For HashiCorp Vault, use the `vault` extra (`forze_vault`) instead.

    :::python
    from forze_kits.secrets import DirectorySecrets, EnvSecrets, MappingSecrets, SecretsDepsModule

    deps = SecretsDepsModule(secrets=EnvSecrets())

`SecretRef.path` is the env var name (`EnvSecrets`), file path under a root directory (`DirectorySecrets`), or key in an in-memory map (`MappingSecrets`). Contract helpers such as `resolve_structured` remain on `forze.application.contracts.secrets`.

## Runtime kit

`DistributedLockScope` wraps `DistributedLockCommandPort` with retry, jitter, optional lease extension, and release on exit:

    :::python
    from forze_kits.runtime import DistributedLockScope

Configure `ttl` on `DistributedLockSpec`; pass `wait_timeout`, `extend_interval`, and `retry_interval` on the scope—not on the spec.

## DTO mapping

The mapping pipeline transforms incoming DTOs before they reach the handler. `PydanticPipelineMapperFactory` maps a Pydantic source model to an output DTO; optional `MappingStep`s inject computed fields such as `number_id` or `creator_id` from `forze_kits.domain`.

## Domain kits

Field and entity shape helpers (`SoftDeletionMixin`, `NumberIdMixin`, `MetadataMixin`, `CreatorIdMixin`, …) are documented in [Domain layer — Mixins](../concepts/domain-layer.md#mixins-forze_kits).
