---
title: Inference
icon: lucide/brain-circuit
summary: Typed model invocation behind one hexagonal seam — local artifact, served endpoint, or cloud, swapped in wiring
---

Calling an ML model from a handler usually means one of three very different
things: invoking an artifact loaded in-process, POSTing to a model server, or
hitting a cloud endpoint. The **inference** port makes all three the same call.
A spec names one logical task with typed input and output models; the wiring
config binds it to a physical model. Handlers pass typed instances and get typed
predictions back — no model URIs, artifact formats, or wire protocols cross the
port, so swapping a pickled sklearn model for a SageMaker endpoint is a wiring
change with zero handler edits.

## The shape in code

One spec = one model. The spec names the *task*, never the artifact:

```python
from forze.application.contracts.inference import InferenceSpec
from pydantic import BaseModel

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

Handlers resolve it off the context and call it like any other port. Both type
parameters propagate, so predictions are fully typed with zero annotations:

```python
port = ctx.inference.model(FRAUD_SCORER)

score = await port.predict(FraudFeatures(amount=120.0, country="NL", velocity_24h=3))
scores = await port.predict_many(batch)          # vectorized, order-preserving
```

The port is **read-plane**: invoking a model is a pure read of it, so a `QUERY`
operation can call it — a query handler computing a recommendation must be able
to score. A scalar prediction wraps in a one-field output model; tensor-shaped
payloads are plain lists of floats inside the models.

Three calls, three shapes:

| Method | Shape | Semantics |
| --- | --- | --- |
| `predict` | one instance → one prediction | the request-path call |
| `predict_many` | batch → batch | vectorized; **all-or-nothing**, order-preserving |
| `predict_stream` | chunk stream → chunk stream | bounded-memory scoring over large sets |

`predict_many` either returns a prediction for every instance or raises for the
whole batch — never a silent partial result. `predict_stream` streams *instance
chunks*, not tokens; every chunk boundary is a deadline check and a cancellation
point.

## In-process models

The local adapter's contract is a **callable, not an artifact format**. You
supply a loader returning an object with a sync `predict_batch`; the framework
only schedules it — loading and every prediction run off the event loop under
the CPU-offload seam (bounded pool, deadline enforcement, cancellation):

```python
from forze.application.integrations.inference import (
    LocalInferenceConfig,
    LocalInferenceDepsModule,
    local_inference_lifecycle_step,
)

def load_fraud_model():
    import joblib                      # your dependency, not the framework's
    return joblib.load("fraud-v3.bin") # exposes predict_batch(instances)

inference_module = LocalInferenceDepsModule(
    models={"fraud_scorer": LocalInferenceConfig(loader=load_fraud_model)},
)
steps = [local_inference_lifecycle_step(inference_module)]
```

The framework never deserializes artifacts itself — unpickling is arbitrary code
execution, and that trust decision stays in your loader. Two behaviors worth
knowing:

- **Warm by default.** `warm_on_startup=True` loads the model at boot through
  the lifecycle step and **fails startup closed** on a loader error: a service
  that would fail its first prediction should not come up. Set it `False` to
  load lazily on first call.
- **Thread safety is your model's contract.** Predictions run on a shared
  worker pool, so concurrent calls hit the same model object from multiple
  threads. sklearn and ONNX Runtime sessions generally tolerate this; for a
  model that does not, set `serialize_calls=True` to route every call through a
  per-route lock — correctness over throughput.

## The mock

`MockDepsModule` answers inference routes from a **pure sync function** you
register — deterministic by contract, so simulation replays stay exact:

```python
from forze_mock import MockDepsModule, MockInferenceRegistry

registry = MockInferenceRegistry().on(
    "fraud_scorer",
    lambda instances: [{"risk": min(1.0, i.amount / 1000)} for i in instances],
)
module = MockDepsModule(inference=registry)
```

An unprogrammed route fails closed (`code="mock.inference.unprogrammed"`).
Outputs pass through the same boundary shaping as every real adapter, so a
mis-shaped stub fails under the mock exactly where a mis-shaped backend would
fail in production.

## Capabilities and errors

Backends diverge, and the port says so declaratively instead of pretending
uniformity: each adapter publishes `inference_capabilities` (native batching, a
hard batch cap, chunked streaming, offline jobs, a determinism promise), and a
request that strays is refused up front with a clean precondition
(`inference_feature_unsupported`) naming the feature and backend — never a
silent degradation. The failure taxonomy at the boundary:

| Condition | Kind | Code |
| --- | --- | --- |
| Instance is not the spec's input model | `validation` | `core.validation` |
| Backend response doesn't fit the output model | `validation` | `inference_output_mismatch` |
| Feature the backend lacks | `precondition` | `inference_feature_unsupported` |
| Per-call timeout / invocation deadline expired | `timeout` | `cpu_offload_deadline` (local) |

Per-call options tighten, never extend: `options={"timeout": timedelta(...)}`
binds a deadline that is the earlier of the per-call budget and the ambient
invocation deadline.

## Features in traces

Simulation value capture masks every input field by default:

```python
FRAUD_SCORER = InferenceSpec(
    name="fraud_scorer", input=FraudFeatures, output=FraudScore,
    capture_inputs=True,   # opt in to verbatim features on captured traces
)
```

Features cannot be field-encrypted — the model needs real values, which is why
external routes must acknowledge data egress — and they are usually PII-dense,
so a captured trace shows `"<redacted>"` in their place unless you set
`capture_inputs=True`. This only affects runtime tracing and simulation;
production traces are id-only regardless. It is worth knowing because a DST
bundle is an artifact that gets stored and shared.

## Resilience

Inference has an unusually good fit with the framework's
[resilience policies](../running-in-prod/resilience.md), because a prediction is
a *pure read*: it mutates nothing, so the strategies that are normally unsafe to
apply are safe here. Bind a policy to the inference dep key and every resolved
port runs under it:

```python
from forze.application.contracts.inference import InferenceDepKey
from forze.application.contracts.resilience import PortPolicy

ResilienceDepsModule(
    spec=my_policies,                       # defines "model_calls"
    port_policies=(PortPolicy(key=InferenceDepKey, policy="model_calls"),),
)
```

Three things are worth tuning deliberately:

- **Hedging.** Models have a long tail — a cold endpoint, an unlucky queue, a
  slow batch. A [hedge](../running-in-prod/resilience.md#hedging-the-tail) races
  a second attempt and takes whichever returns first. It is normally restricted
  to idempotent reads, which every `predict` is. The real cost is money rather
  than correctness: a hedged call to a paid endpoint is billed twice, so set
  `budget` to cap the extra load, and prefer `adaptive_delay_quantile` so the
  hedge tracks the model's actual p95 instead of a guess that ages badly.
- **Criticality.** One policy serves every tier, and the tier rides the call
  context — so a `BEST_EFFORT` recommendation sheds ahead of a `CRITICAL` fraud
  check under load without duplicating any wiring. Set it with
  `bind_criticality` on the caller; see
  [load shedding](../running-in-prod/load-shedding.md#shedding-the-right-requests).
- **Retries.** The taxonomy above already says what is worth retrying:
  `throttled` and `infrastructure` are, `validation` and `precondition` are not.
  A retried call re-runs the whole `predict` — including the CPU offload for a
  local model — so keep `max_batch_size` in mind when retrying big batches.

One caveat specific to this port: `predict_stream` is an async generator, so it
gets the **circuit breaker only**. Retry, hedging, timeout, bulkhead, and rate
limiting never apply to it — a partially consumed stream cannot be replayed. Put
the policy pressure on `predict` / `predict_many`, and treat a long scoring
stream as something you bound with deadlines instead.

## What this seam is not

No training, no experiment tracking, no model registry, no agent loops. The
contract is deliberately *invocation only* — an opaque, versioned, possibly
non-deterministic function artifact with typed IO. Anything that produces or
manages artifacts lives outside; the seam just calls them. The
[remote adapters](../integrations/inference.md) (`forze_inference`) bind the
same spec to served (KServe V2 / MLflow) and cloud (SageMaker) models — and for
text-embedding models specifically, use the dedicated embeddings provider port
(`ctx.embeddings.provider(spec)`) that [vector search](reading-data.md) already
consumes.
