# RFC 0004 — Offline batch inference plane

- **Status:** 📝 Draft — proposed, not started. The follow-up the inference seam promised for the offline batch plane; direction was fixed there, this locks the full design.
- **Scope:** Score large datasets **offline**: submit a job over object-storage locations, poll its status, read results back through the app's normal storage route. A command-plane/read-plane port pair in `contracts/inference` (same area, same `InferenceSpec`), a deterministic mock, and a SageMaker Batch Transform adapter in `forze_inference.sagemaker`. Bytes never traverse Forze for cloud batch — the backend reads and writes storage natively; the seam governs *naming, tenancy, typing and lifecycle*, not the data path.
- **Related:** the inference seam's offline-batch direction (port split, `NamedResourceSpec` locations, durable-memoized submits, `supports_async_jobs` gate) + decisions #4/#13/#14. Locations — [`contracts/resolution`](../src/forze/application/contracts/resolution/__init__.py) (`NamedResourceSpec`, per-tenant resolvers, tenancy-tier derivation). Reliability composition — `DurableFunctionStepPort` ([[durable-execution-rfc]]): memoized `submit` inside a durable step survives crash-retry without double-launching a paid job. Results consumption — `StorageQueryPort` streaming reads. The online seam this extends — [[inference-seam-rfc]] (EXECUTED). Mock batch behavior builds on `MockInferenceRegistry` + `MockStorage`.
- **Origin:** the inference seam deliberately shipped online inference only; "batch" at the contract level means two different things, and only micro-batch (`predict_many` / `predict_stream`) belongs on `InferencePort`. Offline jobs (SageMaker Batch Transform-class: millions of rows, results materialize later) are an async-result workflow over datasets — a different lifecycle, different CQRS plane (submitting launches paid external work), and a data path that bypasses the framework entirely.

---

## 1. Summary

```python
# submit from inside a durable step — the memoized ref survives crash-retry
step = resolve_durable_step(ctx)
job = await step.run(
    "submit-scoring",
    lambda: ctx.inference.batch(FRAUD_SCORER).submit(
        input_key="scoring/2026-07-20.jsonl",
        output_prefix="scored/2026-07-20/",
    ),
)
status = await ctx.inference.batch_status(FRAUD_SCORER).status(job)
```

The spec is the same logical task as online inference (`FRAUD_SCORER`); the batch route config declares the physical facts — input/output **locations** (`NamedResourceSpec`: bucket or per-tenant resolver), the dataset framing, and the backend job parameters. Handlers pass only relative keys. Results land in object storage and are read back through the existing storage ports; "same bucket as the app's storage route" is expressed by both configs referencing the same `NamedResourceSpec` value in the composition root — single source of truth, coupling confined to wiring (the inference seam's offline-batch decomposition, unchanged).

## 2. Contracts (`contracts/inference`, additive)

### 2.1 Ports — command/read split (CQRS)

```python
@runtime_checkable
class BatchInferenceCommandPort(Protocol):
    """Submitting launches paid external work — command-plane, refused in QUERY."""

    spec: InferenceSpec[Any, Any]

    def submit(
        self,
        *,
        input_key: str,
        output_prefix: str,
        job_token: str | None = None,
        options: BatchRunOptions | None = None,
    ) -> Awaitable[BatchJobRef]: ...

    def cancel(self, job: BatchJobRef) -> Awaitable[None]: ...


@runtime_checkable
class BatchInferenceQueryPort(Protocol):
    spec: InferenceSpec[Any, Any]

    def status(self, job: BatchJobRef) -> Awaitable[BatchJobStatus]: ...
```

Dep keys `inference_batch_command` / `inference_batch_query` (metadata inference derives `domain="inference"`, `phase="command"|"query"` from the suffixes — same free observability as the online port). Accessors: `ctx.inference.batch(spec)` via `_resolve_command`, `ctx.inference.batch_status(spec)` via `_resolve_configurable`. Both routes gate on `supports_async_jobs` at resolve (fail-closed `inference_feature_unsupported` for backends that don't claim it — local, http).

### 2.2 Value objects — JSON-trivial by design

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BatchJobRef:
    job_id: str            # backend-native id / name
    route: str             # spec name, for admin/debug symmetry

class BatchJobState(StrEnum):
    PENDING = "pending"; RUNNING = "running"; COMPLETED = "completed"
    FAILED = "failed"; CANCELLED = "cancelled"

@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BatchJobStatus:
    state: BatchJobState
    detail: str | None = None          # backend failure reason, truncated
    output_prefix: str | None = None   # where results landed (relative key prefix)
```

Flat strings/enums only — journalable by `DurableFunctionStepPort` unmodified (the JSON-only step-result constraint is a hard requirement here, not a preference).

### 2.3 Dataset framing (area-local VO — deliberately *not* a shared primitive)

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BatchDatasetFormat:
    content_type: str = "application/jsonlines"   # SageMaker ContentType
    line_delimited: bool = True                    # SplitType=Line / one record per line
```

One record = one JSON object matching `spec.input` (input) / `spec.output` (output) — the same JSON-record scope as the online adapters. Promotion to a shared "typed record dataset" primitive waits for a second consumer (the inference seam's stance; the `IngestSpec` precedent).

### 2.4 Idempotency — `job_token` (revises the inference seam's decision #14, compatibly)

`submit(job_token=…)`: when given, the adapter derives the backend job name from it, and a backend that detects name reuse (SageMaker: `CreateTransformJob` with an existing name fails `ResourceInUse`) returns the **existing** job's ref instead of erroring — natural dedup. When omitted, the adapter generates a name and the durable-step memoization doctrine applies unchanged. Rationale for adding it now rather than later: SageMaker requires a job name *anyway*, and an optional keyword is compatible forever while a later addition to an implemented Protocol is not (the inference seam's decision #2 lesson). Docs still recommend durable-step submission as the primary crash-safety mechanism.

## 3. SageMaker Batch Transform adapter (`forze_inference.sagemaker`, same extra)

```python
SageMakerBatchConfig(TenantAwareIntegrationConfig):
    model_name: NamedResourceSpec            # SageMaker Model (per-tenant capable)
    input_location: NamedResourceSpec        # S3 bucket (or per-tenant resolver)
    output_location: NamedResourceSpec
    format: BatchDatasetFormat = BatchDatasetFormat()
    instance_type: str = "ml.m5.large"
    instance_count: int = 1
    max_payload_mb: int | None = None
    acknowledge_data_egress: bool = False    # must be True; same fail-closed stance
```

- `submit` → `CreateTransformJob` with `S3Uri = s3://{resolve(input_location, tenant)}/{input_key}`, `S3OutputPath = s3://{resolve(output_location, tenant)}/{output_prefix}`, `SplitType`/`ContentType` from `format`, job name from `job_token` or a generated one. `ResourceInUse` on a token-named job → return the existing ref (§2.4).
- `status` → `DescribeTransformJob` mapped onto `BatchJobState` (`InProgress→running`, `Completed→completed`, `Failed→failed` with `FailureReason` as detail, `Stopping/Stopped→cancelled`); `cancel` → `StopTransformJob`.
- Kernel: the existing `SageMakerRuntimeClient` grows nothing — batch talks to the `sagemaker` **control-plane** client (a sibling wrapper in the same kernel, same lifecycle step, own low-level client), keeping data-plane and control-plane clients separate (port-plane-separation doctrine applied to clients).
- Tenancy: `tenant_aware` fail-closed as everywhere; per-tenant buckets/models = namespace tier via the resolvers.
- **Encryption hazard restated:** a client-side-encrypting storage route feeding `input_location` gives the backend ciphertext; wiring cannot cross-validate a raw location. Docs warning (the accepted cost of decoupling, per the inference seam); an optional by-value cross-check hook remains a non-breaking future addition.

## 4. Mock (deterministic, storage-integrated)

`MockBatchInference` completes **synchronously at `submit`** (deterministic — no background progression for DST to chase): it reads line-delimited JSON records from `MockStorage` at `input_key` (same tenant partitioning), decodes them through `spec.resolved_input_codec`, runs the route's `MockInferenceRegistry` function (the same pure function the online mock uses — one stub covers both planes), encodes predictions through the output codec, and writes `{output_prefix}predictions.jsonl` back to `MockStorage`. `status` returns `completed` (or `failed` with the raised error's summary when the function throws — fault injection stays function-level). `job_token` reuse returns the recorded ref. This makes the mock a true end-to-end oracle: a portable batch flow (write dataset → submit → poll → read results) runs fully in-process.

## 5. Phases

| Phase | Deliverable |
| --- | --- |
| P1 | Contracts (ports, VOs, accessors, capability gating) + mock + docs section in `data-events/inference.md` + a durable-step recipe example |
| P2 | `forze_inference.sagemaker` batch adapter (control-plane kernel client, config, factory/module wiring) + unit tests over a stub client |
| P3 | Integration evidence per the managed-cloud fidelity policy: emulator spike (floci/LocalStack SageMaker fidelity) → env-gated real-cloud test if no admissible emulator; mock↔adapter *contract* conformance (state mapping, token reuse) regardless |
| Deferred | Self-hosted batch runner (a kits loop composing `predict_stream` over storage streams — no new contracts needed); provider LLM batch APIs mapping onto these ports (RFC 0006's concern) |

## 6. Decision log

| # | Decision |
| --- | --- |
| 1 | Same `InferenceSpec`, same area; batch = two new ports, never methods on `InferencePort` |
| 2 | Dep keys `inference_batch_command` / `inference_batch_query`; submit/cancel command-plane, status read-plane |
| 3 | Locations via `NamedResourceSpec` only — no storage-contract coupling; shared values in the composition root |
| 4 | `job_token` optional keyword on `submit` from day one (backend job name + natural dedup); durable memo stays the recommended crash-safety |
| 5 | VOs flat/JSON-trivial (durable journaling constraint is load-bearing) |
| 6 | Mock completes at submit, reuses `MockInferenceRegistry` + `MockStorage` (one stub, both planes; deterministic for DST) |
| 7 | `BatchDatasetFormat` stays area-local until a second consumer exists |
| 8 | SageMaker control-plane client is a sibling of the runtime client, not a widening of it |
