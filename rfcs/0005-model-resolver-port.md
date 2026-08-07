# RFC 0005 — `ModelResolverPort` (lite: design lock, demand-gated)

- **Status:** 📝 Draft — design locked, **not scheduled**. Build on first concrete demand (a registry-driven deployment: staging/prod aliases, per-tenant model assignment maintained outside wiring, registry-gated rollout).
- **Scope:** A control-plane seam mapping a *logical* model reference (the spec's task name + an optional alias/stage) to a *physical* target (model name / endpoint / artifact URI) at resolve time — so "which version serves `fraud_scorer`" can live in a registry (MLflow Model Registry, a config service, a table) instead of requiring a wiring change + redeploy. Pure wiring-layer evolution: **zero contract changes to `InferencePort`/`InferenceSpec`**, per the inference seam's deferral list.
- **Related:** [[inference-seam-rfc]] decision #18-adjacent deferral (§10.1). The mechanism it slots into — `NamedResourceSpec = str | ValueResolver[str]` ([`contracts/resolution`](../src/forze/application/contracts/resolution/specs.py)): every remote inference config's `model_name`/`endpoint_name` already accepts a resolver callable, and `resolve_scoped_namespace` already handles static-memoize vs dynamic-per-call. Procedure-style governance (physical binding in wiring, never in handlers) is preserved — the registry moves the binding one level out, it does not hand it to handlers.

---

## 1. Design (locked)

### 1.1 Port — control-plane, read-only

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedModel:
    target: str                    # backend-native name: served model id / endpoint / URI
    version: str | None = None     # informational (traced, not dispatched on)

@runtime_checkable
class ModelResolverPort(Protocol):
    """Map (task, alias, tenant) -> the physical model target. Read-only control plane."""

    def resolve_model(
        self,
        task: str,                          # the InferenceSpec name
        *,
        alias: str = "production",          # registry stage/alias
        tenant_id: UUID | None = None,
    ) -> Awaitable[ResolvedModel]: ...
```

Dep key `model_resolver` (`SimpleDepPort` — one resolver per runtime; routed variants only if a real need appears). Lives in `contracts/inference` beside the capability model.

### 1.2 Integration — through `NamedResourceSpec`, not through new config fields

No adapter or factory changes. A helper bridges the port into the existing resolver slot:

```python
def resolver_backed(task: str, *, alias: str = "production") -> NamedResourceSpec:
    """A ValueResolver that consults the wired ModelResolverPort per resolution."""
```

Usage: `HttpInferenceConfig(model_name=resolver_backed("fraud_scorer"), …)` / `SageMakerInferenceConfig(endpoint_name=resolver_backed(…))` / a local-adapter loader that calls it for an artifact URI. Because the result is a *dynamic* resolver, `resolve_scoped_namespace` already re-resolves per call (no stale memoization — the same rule tenant-scoped resolvers follow); a deployment that wants per-process pinning wraps it in its own cache with an explicit TTL. Implementation note: the helper needs the resolved port, so it is produced by a small factory at wiring time (where `ctx.deps` is reachable), mirroring how adapter factories thread `tenant_provider` today.

### 1.3 Adapters (when built)

- `kits`-level `MappingModelResolver` (static dict / env-backed) — the zero-infra reference + mock story (deterministic).
- `forze_inference.mlflow_registry` (or a submodule of `http`) — MLflow Model Registry aliases → model URI/name; the first real backend, only when demanded.

### 1.4 Fail-closed rules

- `resolver_backed` with no `model_resolver` registered → `exc.configuration` at first resolution, naming the missing key.
- A resolver returning an empty/blank target → `exc.configuration` (never a silent fallback to some default model).
- Resolution failures are `configuration`/`infrastructure` per cause; **never** silently pin the last-known target (a stale model served silently is the failure mode registries exist to prevent).

## 2. Non-goals

- **No per-call model selection** — `alias` is config, handlers still pass typed instances only (governance unchanged).
- **No traffic splitting / canary logic** in the seam — that is the serving platform's job (`target_variant`, KServe canary), already expressible as config.
- **No model *management*** (register/promote/delete) — read-only resolution; management stays in the registry's own tooling.

## 3. Decision log

| # | Decision |
| --- | --- |
| 1 | Wiring-layer only; zero `InferencePort`/`InferenceSpec` changes |
| 2 | Integration via `NamedResourceSpec` + `resolver_backed` helper — no new config fields, all three adapters covered for free |
| 3 | `SimpleDepPort` under `model_resolver`; routed variants demand-gated |
| 4 | Dynamic-resolver semantics (per-call re-resolution) — pinning/TTL is the caller's explicit wrapper |
| 5 | Fail-closed: missing resolver, blank target, and resolution failure all raise; no silent last-known fallback |
| 6 | Demand-gated: this document locks the design so pickup is a single small PR, nothing more |
