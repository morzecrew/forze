# Inference

Invoking a model through an `InferenceSpec` — the spec names a task, wiring names the artifact. Covers local, HTTP and SageMaker backends, capability refusals, and the mock registry.

## Declare the task, not the artifact

```python
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec


class FraudFeatures(BaseModel):
    amount: float
    country: str
    velocity_24h: int


class FraudScore(BaseModel):
    risk: float


FRAUD_SCORER = InferenceSpec(
    name="fraud_scorer",
    input=FraudFeatures,
    output=FraudScore,
)
```

One spec = one model. Scalar predictions wrap in a one-field output model; tensor
payloads are plain lists of floats inside the models.

## Call it from a handler

```python
port = ctx.inference.model(FRAUD_SCORER)

score = await port.predict(FraudFeatures(amount=120.0, country="NL", velocity_24h=3))
scores = await port.predict_many(batch)                 # order-preserving, all-or-nothing

async for chunk in port.predict_stream(instance_chunks):  # bounded memory
    ...
```

The port is **read-plane** — invoking a model mutates nothing, so a `QUERY`
operation may call it. `predict_many` returns a prediction for every instance or
raises for the whole batch, never a silent partial. `predict_stream` streams
*instance chunks*, not tokens; every chunk boundary is a deadline check and a
cancellation point.

Per-call options (`InferenceRunOptions`) tighten, never extend, and carry no
model-targeting fields — which model a route invokes is a wiring fact:

```python
score = await port.predict(features, options={"timeout": timedelta(seconds=2)})
```

The effective deadline is the earlier of that budget and the ambient invocation
deadline.

## Backends

| Backend | Module | Use it for |
|---|---|---|
| Local | `LocalInferenceDepsModule` (`forze.application.integrations.inference`) | an artifact loaded in-process |
| HTTP | `HttpInferenceDepsModule` (`forze_inference.http`, extra `inference-http`) | KServe-v2 / mlserver / Seldon / Triton, or MLflow `/invocations` |
| SageMaker | `SageMakerInferenceDepsModule` (`forze_inference.sagemaker`, extra `inference-sagemaker`) | an AWS endpoint |
| Mock | `MockDepsModule(inference=MockInferenceRegistry())` | tests and simulation |

```python
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    inference_http_lifecycle_step,
)

module = HttpInferenceDepsModule(
    client=InferenceHttpClient(),
    models={
        "fraud_scorer": HttpInferenceConfig(
            protocol="kserve_v2",              # or "mlflow"
            model_name="fraud-scorer",         # static, or (tenant_id) -> name
            acknowledge_data_egress=True,      # required; wiring fails closed without it
        ),
    },
)
steps = [inference_http_lifecycle_step("http://mlserver:8080")]
```

Local models take a **callable, not an artifact format** — you supply a loader
returning an object with a sync `predict_batch`, and the framework schedules it off
the event loop under the CPU-offload seam. The framework never deserializes
artifacts itself; unpickling is arbitrary code execution and that trust decision
stays in your loader.

```python
LocalInferenceDepsModule(
    models={"fraud_scorer": LocalInferenceConfig(loader=load_fraud_model)},
)
```

`warm_on_startup=True` (the default) loads at boot through
`local_inference_lifecycle_step` and **fails startup closed** on a loader error.
Predictions share a worker pool, so concurrent calls hit the same model object from
multiple threads — set `serialize_calls=True` for a model that cannot tolerate it.

## Capabilities fail closed

Backends diverge and the port says so instead of pretending uniformity. Each
adapter publishes `inference_capabilities` (native batching, a hard batch cap,
chunked streaming, a determinism promise); a request that strays is refused up
front with `inference_feature_unsupported`, naming the feature and backend — never
silently degraded. The boundary taxonomy:

| Condition | Kind | Code |
|---|---|---|
| Instance is not the spec's input model | `validation` | `core.validation` |
| Backend response does not fit the output model | `validation` | `inference_output_mismatch` |
| Feature the backend lacks | `precondition` | `inference_feature_unsupported` |
| Per-call timeout or invocation deadline expired | `timeout` | `cpu_offload_deadline` (local) |

Upstream error bodies are withheld from errors and logs; an upstream 401/403 and a
failing container classify as infrastructure.

## Tenancy and data egress

Features cross the process boundary **in plaintext** — the model needs real values,
so field encryption cannot apply. Every remote config therefore requires
`acknowledge_data_egress=True`.

All four isolation tiers are honored. Pass `required_tenant_isolation=` to the deps
module and wiring refuses anything weaker: `none`, `tagged` (a bound tenant
required), `namespace` (a `(tenant_id) -> name` resolver for the model or endpoint
name), or `dedicated` (a routed client per tenant, resolved from that tenant's own
secret — `RoutedInferenceHttpClient` / `RoutedSageMakerRuntimeClient`).

## Testing

Register a **pure sync function** per route — deterministic by contract, so
simulation replays stay exact:

```python
from forze_mock import MockDepsModule, MockInferenceRegistry

registry = MockInferenceRegistry().on(
    "fraud_scorer",
    lambda instances: [{"risk": min(1.0, i.amount / 1000)} for i in instances],
)
module = MockDepsModule(inference=registry)
```

An unprogrammed route fails closed (`mock.inference.unprogrammed`). Outputs pass
through the same boundary shaping as a real adapter, so a mis-shaped stub fails
exactly where a mis-shaped backend would.

Simulation value capture **masks inference inputs by default** (features are
usually PII-dense and a trace bundle gets stored and shared). Opt in per spec with
`capture_inputs=True`.

## Resilience

A prediction is a pure read, which makes strategies that are normally unsafe safe
here. Bind a policy to `InferenceDepKey` and every resolved port runs under it.
Hedging fits the long tail well — prefer `adaptive_delay_quantile` over a fixed
delay, and set `budget`, because a hedged call to a paid endpoint is billed twice.
Retry `throttled` and `infrastructure`; never `validation` or `precondition`.

## Anti-patterns

- **Putting the model URI, endpoint, or artifact path in the spec** — the spec names a task; the physical model is wiring.
- **Importing an ML library in a handler** — the loader is the only place your model's dependency belongs.
- **Blocking the event loop with a sync `predict`** — use the local adapter's loader seam so scoring runs off-loop under the deadline.
- **Expecting `predict_many` to return partial results** — it is all-or-nothing by contract; batch on your side if you need per-instance failure.
- **Wiring a remote route without `acknowledge_data_egress=True`** — it fails closed on purpose; state the decision instead of working around it.
- **Assuming a backend supports streaming or unbounded batches** — read the capabilities; the refusal is up front, not at scale.
- **Enabling `capture_inputs=True` on PII-dense features** without deciding who will read the trace bundle.

## Reference

- [Inference](https://morzecrew.github.io/forze/latest/data-events/inference/)
- [Inference integrations](https://morzecrew.github.io/forze/latest/integrations/inference/)
- [Offload CPU work](https://morzecrew.github.io/forze/latest/recipes/offload-cpu-work/)
