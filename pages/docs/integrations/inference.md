---
title: Remote inference
icon: lucide/brain-circuit
summary: forze_inference — served (KServe V2 / MLflow) and cloud (SageMaker) models behind the inference seam
---

`forze_inference` binds [inference](../data-events/inference.md) routes to remote
models. One submodule per backend, each behind its own extra, all implementing the
same port — handlers never change when a model moves from an in-process artifact
to a served endpoint or a cloud one.

| Submodule | Extra | Speaks to |
| --- | --- | --- |
| `forze_inference.http` | `forze[inference-http]` | KServe, mlserver, Seldon, Triton (Open Inference Protocol); legacy MLflow `/invocations` |
| `forze_inference.sagemaker` | `forze[inference-sagemaker]` | AWS SageMaker realtime endpoints |

Both are **JSON-record** adapters in this release: instances and predictions
travel as JSON records built from your spec's Pydantic models. Binary tensor
encodings are a planned extension, not a silent fallback — a spec the encoding
cannot represent is refused at wiring.

## Served models over HTTP

```python
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    inference_http_lifecycle_step,
)

client = InferenceHttpClient()

module = HttpInferenceDepsModule(
    client=client,
    models={
        "fraud_scorer": HttpInferenceConfig(
            protocol="kserve_v2",            # or "mlflow"
            model_name="fraud-scorer",       # server-side model id
            acknowledge_data_egress=True,
        ),
    },
)
steps = [inference_http_lifecycle_step("http://mlserver:8080")]
```

`protocol="kserve_v2"` is the default choice — it covers everything that speaks
the Open Inference Protocol. Input fields map to named columnar tensors (the
`content_type: "pd"` convention), so the spec's input model must hold **flat
scalar fields** (`bool` / `int` / `float` / `str`); anything else is refused at
wiring with the offending fields named. `protocol="mlflow"` posts
`{"instances": [...]}` records and accepts nested models.

`model_name` accepts a static name or a `(tenant_id) -> name` resolver for
per-tenant models — the same namespace-tier pattern as a per-tenant bucket or
database. A `tenant_aware=True` route with no bound tenant fails closed
(`tenant_required`).

## SageMaker

```python
from forze_inference.sagemaker import (
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
    SageMakerRuntimeClient,
    sagemaker_inference_lifecycle_step,
)

module = SageMakerInferenceDepsModule(
    client=SageMakerRuntimeClient(),
    models={
        "fraud_scorer": SageMakerInferenceConfig(
            endpoint_name="fraud-scorer-prod",
            target_variant="blue",           # optional variant pin
            acknowledge_data_egress=True,
        ),
    },
)
steps = [sagemaker_inference_lifecycle_step(region_name="eu-west-1")]
```

Requests send `{"instances": [...]}` and expect `{"predictions": [...]}` — the
TF-Serving / sklearn container convention. Credentials default to the botocore
chain; `endpoint_name` is per-tenant capable like `model_name` above.

## What both adapters guarantee

- **Explicit data egress.** Features leave the encryption boundary in plaintext
  by necessity — the model needs real values. Every remote config therefore
  requires `acknowledge_data_egress=True`; wiring fails closed until the
  operator states it.
- **A declarable tenancy floor.** Pass `required_tenant_isolation=` to either
  deps module and wiring refuses anything weaker: `none` (one shared model),
  `tagged` (a bound tenant is required, still one shared model), `namespace` (a
  per-tenant `model_name` / `endpoint_name` resolver — a model per tenant behind
  one connection), or `dedicated` (below).

## Per-tenant models (`dedicated`)

`namespace` gives each tenant its own model *name*; every tenant's features
still travel over one shared client and one shared credential. `dedicated`
resolves a whole client per tenant from that tenant's own secret:

```python
from forze_inference.http import (
    RoutedInferenceHttpClient,
    routed_inference_http_lifecycle_step,
)

client = RoutedInferenceHttpClient(
    secrets=secrets,                                   # SecretsPort
    secret_ref_for_tenant=lambda t: SecretRef(path=f"tenants/{t}/inference"),
    tenant_provider=current_tenant,
)
steps = [routed_inference_http_lifecycle_step(client)]
```

The secret holds `InferenceHttpRoutingCredentials` — `base_url` plus optional
`headers` / `bearer_token` — so a tenant's features reach *that tenant's* model
server and nothing else. `RoutedSageMakerRuntimeClient` is the AWS counterpart:
its `SageMakerRoutingCredentials` carry `region_name` and static access keys, so
each tenant invokes under its own AWS identity and endpoint access is enforced
by IAM rather than only by the endpoint name your app resolved. Static
credentials are required there — falling back to the ambient botocore chain
would put every tenant back on one principal, defeating the isolation.

Clients are built lazily per tenant and cached (`max_cached_tenants`, LRU);
rotating a tenant's secret changes its fingerprint and rebuilds that client
transparently. Calls with no bound tenant fail closed rather than picking a
default.
- **All-or-nothing batches.** `predict_many` is one wire call
  (`native_batch=True`); with a configured `max_batch_size` an oversized batch
  is refused whole, never silently split. `predict_stream` *does* sub-batch its
  wire calls to the cap — while preserving your chunk boundaries.
- **Typed boundary.** Responses decode through the spec's output codec; a
  response that doesn't fit raises `inference_output_mismatch` at the port, and
  scalar predictions wrap into a one-field output model automatically.
- **Error taxonomy.** Endpoint throttle → `throttled` (`inference_throttled`),
  unknown endpoint/model → `configuration` (`inference_route_mismatch`),
  payload rejection / model error → `validation` (`inference_output_mismatch`),
  unreachable / 5xx → `infrastructure` (`inference_endpoint_unavailable`),
  budget expiry → `timeout` (`inference_timeout`). Retryability follows the
  standard egress policy, so resilience retries the right ones.
- **Deadlines propagate.** The remaining invocation budget bounds every wire
  call; a per-call `options={"timeout": ...}` can only tighten it.
