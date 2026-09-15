---
title: Remote inference
icon: lucide/brain-circuit
summary: forze_inference — served (KServe V2 / MLflow), generative (OpenAI chat) and cloud (SageMaker) models behind the inference seam
---

`forze_inference` binds [inference](../data-events/inference.md) routes to remote
models. One submodule per backend, each behind its own extra, all implementing the
same port — handlers never change when a model moves from an in-process artifact
to a served endpoint or a cloud one.

| Submodule | Extra | Speaks to |
| --- | --- | --- |
| `forze_inference.http` | `forze[inference-http]` | KServe, mlserver, Seldon, Triton (Open Inference Protocol); legacy MLflow `/invocations`; OpenAI-style `/v1/chat/completions` |
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

## Generation (`openai_chat`)

The third dialect covers anything speaking `/v1/chat/completions` — OpenAI, vLLM,
Ollama, LM Studio, TGI, Groq, OpenRouter, Azure, Together. One dialect, no
provider SDK, no second client: a generative call is an inference route, so it
carries the enclosing operation's deadline, the route's tenant and credentials,
the egress declaration, the resilience policy — and it can be simulated, which a
call made from a vendor client cannot.

```python
from forze.application.contracts.inference import InferenceSpec
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    PromptTemplate,
)
from pydantic import BaseModel


class TicketText(BaseModel):
    text: str


class Triage(BaseModel):
    queue_id: str
    weight: int


TRIAGE = InferenceSpec(name="ticket_triage", input=TicketText, output=Triage)

module = HttpInferenceDepsModule(
    client=InferenceHttpClient(),
    models={
        "ticket_triage": HttpInferenceConfig(
            protocol="openai_chat",
            model_name="gpt-5",                  # or a (tenant_id) -> name resolver
            prompt=PromptTemplate(
                system="You triage support tickets.",
                template="Triage this ticket:\n\n{text}",
            ),
            temperature=0.0,
            acknowledge_data_egress=True,
        ),
    },
)
```

Point the lifecycle step at the server **root** (`https://api.openai.com`,
`http://vllm:8000`) — the dialect owns the `/v1/chat/completions` path.

**The prompt is wiring.** A handler calls
`ctx.inference.model(TRIAGE).predict(TicketText(text=...))` and gets a `Triage`;
it never sees a prompt, a model id or a provider, the same way the procedure
plane keeps its SQL in the composition root. Template slots are `str.format`
field names bound from the input instance (`{text}`), and they are checked
against the input model at resolve time: a slot no field provides, or a template
with no slots at all, is a boot error rather than a malformed prompt in
production. Positional and attribute slots (`{0}`, `{a.b}`) are refused, so a
slot always names one field.

**Sampling is configuration.** `temperature` and `max_output_tokens` live on the
route, never in `options=`, so what the model does is a reviewed deployment fact.

### Structured and text modes

`output_mode` defaults to `structured`: the output model's JSON schema is sent as
a strict `response_format` constraint, so the route answers with a validated
`Out`. The constraint enforces less than JSON Schema can express, and the
difference fails **at wiring**, naming every offending field:

- A field with a **default** is refused. Under the constraint a field cannot be
  absent, so the default is unreachable — and forcing the model to fill it would
  make it invent a value. Declare `T | None` (with no default) for something the
  model may not know, and it must answer `null` explicitly.
- Value constraints (`format`, `pattern`, `max_length`, `ge`/`le`, …) are
  refused, because the provider does not enforce them. A `datetime` or `EmailStr`
  field emits `format` — use `str` and validate after, or drop the constraint.
- `additionalProperties: false` is added for you on every object.

`output_mode="text"` sends no constraint and fills a one-field `str` output model
with the completion prose; any other output model is refused at wiring.

### What a generation route costs and refuses

- **One request per instance.** A chat completion answers one prompt, so
  `predict_many` is N sequential requests (`native_batch=False`) and the budget
  is checked before each one: a fan-out that runs out of deadline stops rather
  than finishing on borrowed time.
- **Usage is telemetry.** Token counts land on the call's span as
  `forze.inference.usage.input_tokens` / `.output_tokens`, summed over the
  fan-out. There is no envelope method — cost accounting does not belong in every
  handler's return type.
- **A safety refusal is not a wire defect.** A provider declining on content
  grounds raises `precondition` (`inference_content_refused`), non-retryable,
  whether it says so in `message.refusal` or in `finish_reason`. A truncated
  completion says which knob to raise.
- **Anthropic: `text` mode only.** Its OpenAI-compatibility endpoint *ignores*
  `response_format` rather than rejecting it, so a structured route there answers
  with prose and fails at the output boundary. Use the native API for Claude's
  structured outputs.

### When to use a vendor SDK instead

This plane serves **typed one-shot generation inside a governed operation** —
extraction, classification, scoring, routing. That is the dominant backend LLM
workload, and the one an SDK gives no governance for.

An app that owns a multi-turn agent loop — conversation state, tool-call
orchestration, streaming tokens to a user — should use its provider's SDK for the
model and [operations as tools](../data-events/agent-tools.md) for the tools. The
SDKs will always cover streaming, prompt caching, reasoning parameters, files and
vision better, because they ship with the provider; this dialect is not trying to,
and intra-response token streaming is not part of it yet.

`examples/recipes/llm_triage_dst/` is the simulability argument as a runnable
example: one operation asks the model to triage a ticket and files the answer
through a governed aggregate, and the same workload runs under `forze_dst` with
`MockInferenceAdapter` standing in for the endpoint — so the model's answer is
explored as untrusted input to an invariant, reproducibly, from a seed.

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

## Settings

`InferenceHttpSettings` holds the model endpoint, default headers and a bearer token
(merged into `Authorization` unless the headers already carry one); `SageMakerSettings`
holds the AWS region and optional static credentials, refusing a half-set pair. See
[connection settings](index.md#connection-settings).

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
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze_inference.http import RoutedInferenceHttpClient

client = RoutedInferenceHttpClient(
    secrets=secrets,                                   # SecretsPort
    secret_ref_for_tenant=lambda t: SecretRef(path=f"tenants/{t}/inference"),
    tenant_provider=current_tenant,
)
steps = [routed_client_lifecycle_step("routed_inference_http_client", client=client)]
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
- **All-or-nothing batches.** With a configured `max_batch_size` an oversized
  `predict_many` is refused whole, never silently split; `predict_stream` *does*
  sub-batch its wire calls to the cap, preserving your chunk boundaries. Whether
  a batch is one wire call is the dialect's to say (`native_batch`): the scoring
  protocols send one, `openai_chat` sends one request per instance.
- **Typed boundary.** Responses decode through the spec's output codec; a
  response that doesn't fit raises `inference_output_mismatch` at the port, and
  scalar predictions wrap into a one-field output model automatically.
- **Error taxonomy.** Endpoint throttle → `throttled` (`inference_throttled`),
  unknown endpoint/model → `configuration` (`inference_route_mismatch`),
  payload rejection / model error → `validation` (`inference_output_mismatch`),
  unreachable / 5xx → `infrastructure` (`inference_endpoint_unavailable`),
  mid-call budget expiry → `timeout` (`inference_timeout`), a budget already
  spent before the call → `timeout` (`inference_budget_exhausted`, raised without
  reaching the endpoint, so it cannot be a billed invocation). Retryability
  follows the standard egress policy, so resilience retries the right ones.
- **Deadlines propagate.** The remaining invocation budget bounds every wire
  call; a per-call `options={"timeout": ...}` can only tighten it.
