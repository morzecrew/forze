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

## Generation (`openai_chat`, `anthropic_messages`)

Two dialects ask a model for a completion. `openai_chat` covers anything speaking
`/v1/chat/completions` — OpenAI, vLLM, Ollama, LM Studio, TGI, Groq, OpenRouter,
Together — and `anthropic_messages` speaks Anthropic's native `/v1/messages`.
Dialects, not clients: a generative call is an inference route, so it carries the
enclosing operation's deadline, the route's tenant and credentials, the egress
declaration, the resilience policy — and it can be simulated, which a call made
from a vendor client cannot.

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
`http://vllm:8000`, `https://api.anthropic.com`) — the dialect owns the path it
speaks, `/v1/chat/completions` or `/v1/messages`. Azure
OpenAI's classic per-deployment URLs
(`/openai/deployments/{deployment}/chat/completions?api-version=…`) are not that
shape and this dialect does not serve them; its newer `/openai/v1` surface is,
with the base URL carrying `/openai`.

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
A temperature above the endpoint's own ceiling — 2 for chat completions, 1 for
the Messages API — is refused at wiring rather than on every request.

### Claude, natively (`anthropic_messages`)

Same config, two differences. The endpoint requires `max_tokens` on every
request, so `max_output_tokens` is **required** here and a route without one is a
boot error — the alternative would be a default that picks the truncation point
for you. And the transport headers are wiring: the dialect sends a path and a
body, so `x-api-key` and `anthropic-version` go on the client's settings.

```python
HttpInferenceConfig(
    protocol="anthropic_messages",
    model_name="claude-opus-5",
    prompt=PromptTemplate(
        system="You extract invoice fields precisely.",
        template="Extract the invoice fields from this document:\n\n{text}",
    ),
    max_output_tokens=1024,
    acknowledge_data_egress=True,
)
```

```yaml
# settings — the endpoint's own requirements, not the dialect's
inference_http:
  base_url: https://api.anthropic.com
  default_headers:
    x-api-key: ${ANTHROPIC_API_KEY}
    anthropic-version: "2023-06-01"
```

A missing `anthropic-version` is a 400 from the endpoint on the first request,
not a boot refusal — the one wiring mistake on this route the plane cannot catch
for you.

**Use this dialect, not the compatibility endpoint, for structured output.**
Anthropic's OpenAI-compatible surface *ignores* `response_format` and ignores a
tool's `strict` flag, so a structured route pointed there answers with prose and
fails at the output boundary. Reach `openai_chat` at Anthropic only for
`output_mode="text"`.

### Structured and text modes

`output_mode` defaults to `structured`: the output model's JSON schema is sent as
the provider's constraint, so the route answers with a validated `Out`. Each
constraint enforces less than JSON Schema can express, and the difference fails
**at wiring**, naming every offending field.

Both dialects refuse these:

- A **mapping** field — `dict[str, int]` or a bare `dict`, and a model with
  `extra="allow"`: the constraint cannot express dynamic keys, and closing them
  for you would leave it permitting nothing but `{}`. And a **non-object root**
  (a `RootModel`, or a root-level union) — wrap it in a model with one field.
- A field with **nothing to constrain**: `Any`, `object`, or a bare `list`, whose
  member schema says nothing. Declare the shape, or use `output_mode="text"`.
- Numeric and length bounds (`ge`/`le`, `max_length`, …), which neither provider
  enforces, so the model would be free to answer outside them.
- `additionalProperties: false` is added for you on every object.

The rest is **per dialect**, because the accepted vocabularies differ — and this
is the practical reason to pick one endpoint over the other for a given model:

| Output model feature | `openai_chat` | `anthropic_messages` |
| --- | --- | --- |
| A field with a **default** (an optional property) | refused | served |
| `datetime`, `EmailStr`, `UUID` (a string `format`) | refused | served |
| A nested model (`$ref`) | served | served |
| `min_length=1` on a list | refused | served |
| `min_length=2` on a list | refused | refused |
| A **self-referencing** model | refused | refused |
| Output model name as the constraint's name | ASCII, ≤ 64 chars | not sent |

Where the chat dialect refuses a `format` outright, the native one enforces a
listed set (`date-time`, `date`, `time`, `duration`, `email`, `hostname`, `uri`,
`ipv4`, `ipv6`, `uuid`) and refuses anything outside it. A `pattern` is the same
story: enforced natively, so it serves — except for the constructs the decoder
does not run (a word boundary, lookaround, a backreference), which are refused
by name.

`output_mode="text"` sends no constraint and fills a one-field `str` output model
with the completion prose; any other output model is refused at wiring.

### What a generation route costs and refuses

- **One request per instance.** A chat completion answers one prompt, so
  `predict_many` is N sequential requests (`native_batch=False`) and the budget
  is checked before each one: a fan-out that runs out of deadline stops rather
  than finishing on borrowed time. Nothing caps the fan-out by default, so a
  generation route is a good place to set `max_batch_size` — an oversized batch
  is then refused up front instead of becoming a hundred billed requests.
- **Usage is telemetry.** Token counts land on the call's span as
  `gen_ai.usage.input_tokens` / `.output_tokens` — OpenTelemetry's GenAI names,
  so a cost dashboard reads them untaught — summed over the fan-out and recorded
  even when a request part-way through fails. There is no envelope method; cost
  accounting does not belong in every handler's return type.
- **A safety refusal is not a wire defect.** A provider declining on content
  grounds raises `precondition` (`inference_content_refused`), non-retryable —
  reported as `message.refusal` or a `finish_reason` by one dialect, and as a
  first-class `refusal` stop reason by the other. Its explanation is withheld:
  it quotes back a prompt built from the caller's own input.
- **An unfinished answer is refused, not returned.** A completion cut off at the
  token ceiling raises `inference_output_mismatch` naming `max_output_tokens`, in
  both modes — it can still parse as JSON or read as prose, so returning it would
  hand back a half answer the caller cannot tell from a whole one. On
  `anthropic_messages` that generalizes: any stop reason other than a finished
  answer or a stop sequence is refused the same way.

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
