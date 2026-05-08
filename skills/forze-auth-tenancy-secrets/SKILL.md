---
name: forze-auth-tenancy-secrets
description: >-
  Uses Forze authn/authz, tenancy, call context, secrets, routed clients, and
  FastAPI/worker identity binding. Use when adding authentication,
  authorization, tenant-aware infrastructure, or secret-backed configuration.
---

# Forze auth, tenancy, and secrets

Use when identity, tenant routing, authorization, or secret resolution affects application behavior. Keep binding at the boundary; usecases read context and resolve ports.

## Boundary binding

`ExecutionContext` stores call, authn, and tenancy state in context variables. Bind them in HTTP middleware, Socket.IO adapters, queue workers, or Temporal interceptors.

```python
from forze.application.execution import CallContext

with ctx.bind_call(
    call=CallContext(execution_id=execution_id, correlation_id=correlation_id),
    identity=authn_identity,
    tenancy=tenant_identity,
):
    await usecase(args)
```

Usecases should call `ctx.get_authn_identity()` / `ctx.get_tenancy_identity()` and should not call `bind_call(...)`.

## FastAPI identity

`ContextBindingMiddleware` can decode trusted identity headers or call resolver ports. `HeaderAuthIdentityResolver` reads bearer tokens / API keys, resolves `AuthnDepKey` for an `AuthnSpec`, and binds `AuthnIdentity`.

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.middlewares import ContextBindingMiddleware
from forze_fastapi.middlewares.context.authn import HeaderAuthIdentityResolver

authn_spec = AuthnSpec(name=AuthnName.DEFAULT)

app.add_middleware(
    ContextBindingMiddleware,
    ctx_dep=ctx_dep,
    authn_identity_resolver=HeaderAuthIdentityResolver(authn=authn_spec),
)
```

## Authn and authz ports

Authn contracts include `AuthnPort`, password lifecycle, token lifecycle, API-key lifecycle, and password account provisioning. Authz contracts include principal registry, role assignment, effective grants, and permission checks.

Resolve custom auth ports through dependency keys:

```python
from forze.application.contracts.authn import AuthnDepKey, PasswordCredentials

factory = ctx.dep(AuthnDepKey, route=authn_spec.name)
authn = factory(ctx, authn_spec)
identity = await authn.authenticate_with_password(
    PasswordCredentials(login=email, password=password)
)
```

`forze_authnz` provides document-backed authn models/services/adapters; its document specs use `StrEnum` resource names and must be wired to document storage like any other aggregate.

## Tenancy and routed clients

`TenantIdentity` is the current tenant. Tenant-aware adapters should derive routing from `ExecutionContext`, not from user DTO fields. Routed Postgres, Mongo, Redis, S3, RabbitMQ, SQS, and Temporal clients can choose per-tenant infrastructure at call time.

For database-per-tenant Postgres routing, set `PostgresDepsModule.introspector_cache_partition_key` so catalog metadata caches are partitioned per tenant/database.

## Secrets

`SecretsDepKey` registers a `SecretsPort`. `SecretRef` is a logical path, and `resolve_structured()` validates JSON secrets into Pydantic models.

```python
from forze.application.contracts.secrets import SecretRef, SecretsDepKey, resolve_structured

secrets = ctx.dep(SecretsDepKey)
dsn = await resolve_structured(secrets, SecretRef("postgres/main"), PostgresDsnSecret)
```

Use secrets for credentials and routed client configuration; avoid putting secret values in specs.

## Anti-patterns

1. **Binding identity inside usecases** — bind at the boundary only.
2. **Passing tenant ids through every DTO for routing** — bind `TenantIdentity` and use tenant-aware adapters.
3. **Hard-coding credentials in deps modules** — resolve via secrets/config.
4. **Treating authz as domain-only state** — use authz ports for policy decisions that depend on external grants.
5. **Forgetting authn document specs need storage wiring** — `forze_authnz` specs are still `DocumentSpec`s.

## Reference

- [`pages/docs/integrations/fastapi.md`](../../pages/docs/integrations/fastapi.md)
- [`src/forze/application/contracts/authn`](../../src/forze/application/contracts/authn)
- [`src/forze/application/contracts/authz`](../../src/forze/application/contracts/authz)
- [`src/forze/application/contracts/tenancy`](../../src/forze/application/contracts/tenancy)
- [`src/forze/application/contracts/secrets`](../../src/forze/application/contracts/secrets)
- [`src/forze_authnz`](../../src/forze_authnz)
