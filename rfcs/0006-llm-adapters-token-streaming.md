# RFC 0006 — LLM adapters + token streaming (the standalone LLM block)

- **Status:** 🚧 In progress — **rewritten 2026-09-14**; the 2026-07 draft is superseded in full and §7 records what changed and why. Tier 0 is now **one wire protocol on the shipped inference-HTTP plane**, not two provider submodules with their own clients. Executes the inference seam's LLM direction (decision #18): LLM converges **in-area**; no parallel `contracts/llm`, ever. **P1 shipped 2026-09-16** (#439): the `openai_chat` dialect and `PromptTemplate` with their wiring refusals, `strict` structured outputs, the `inference_content_refused` row, usage as span attributes, a docs section, and `examples/recipes/llm_triage_dst/` — a simulation whose handler calls the seam and whose green run is backed by an ungoverned contrast, which is §1's hole closed and demonstrably so. Row 13 did not survive contact: Anthropic's compatibility endpoint **ignores** `response_format` rather than rejecting it, so the dialect covers it in `text` mode only (row 14) — row 3's gate still needs a named consumer before a native client is built. Departures and the audit are in [`logs/T-0006.md`](../logs/T-0006.md); rows 14–21 are the rows they proposed. **P2 re-scoped 2026-09-16** (rows 22–25): the compatibility endpoint cannot carry a schema constraint at all — it ignores `response_format` *and* a tool's `strict`, and the provider's own page names the native API as the remedy — so P2 is a second **dialect** (`anthropic_messages`, native `/v1/messages`), not the native SDK client it used to be. That client is now P5 and stays gated, because row 3 always gated a *client* and a dialect is not one. **P2 shipped 2026-09-17** (#440): the `anthropic_messages` dialect over the native `/v1/messages`, with the generation half of the plane extracted into one shared module first, a shared schema walk carrying per-dialect rules, `max_output_tokens` required at wiring, the per-dialect temperature ceiling, and a `pattern` constraint served over the subset the constrained decoder runs. Two shapes the P1 dialect had let through are refused for both now — a field whose schema says `additionalProperties: true` and a field with nothing to constrain at all (row 30). Departures, the audit and eight review rounds are in [`logs/T-0006-P2.md`](../logs/T-0006-P2.md); rows 26–32 are the rows they proposed. **P3–P5 remain.**
- **Scope:** Schema-constrained generation as ordinary inference, by teaching `forze_inference.http` one more dialect: an `openai_chat` `WireProtocol` plus **prompt-as-config** (the template is route wiring, exactly like procedure's SQL). No new package, no provider SDK, no provider-specific client code. Tier 1 — intra-response **token streaming** as an additive sibling port (`GenerationStreamPort`) — stays specified and demand-gated. Agent loops, conversation state, tool orchestration and prompt DSLs are permanently app/kits territory.
- **Related:** the inference seam's LLM direction (tiers fixed 2026-07-13) and its locked decisions (all-or-nothing `predict_many`, `Out` model-only, egress-ack, capabilities). [`forze_inference/http/protocols/base.py`](../src/forze_inference/http/protocols/base.py) — the `WireProtocol` strategy this RFC extends (`kserve_v2` 171 lines, `mlflow` 52); [`configs.py`](../src/forze_inference/http/execution/deps/configs.py) — `HttpInferenceConfig.protocol`, its `wire_protocol()` factory, its `validate_against_spec` hook, and the egress acknowledgement it already enforces. [`MockInferenceAdapter`](../src/forze_mock/adapters/inference.py) — the simulated half of this seam, already shipped. Prompt-as-config precedent — [`procedure/specs.py`](../src/forze/application/contracts/procedure/specs.py). Delta delivery — the realtime egress plane ([realtime.md](../pages/docs/data-events/realtime.md)). [RFC 0022](0022-sensitive-egress-gate.md) (the egress marker, and the doctrine that forze owns no loop), [RFC 0023](0023-operation-tool-bridge.md) (the inbound half: an agent's tools are governed operations, proved under simulation).
- **Origin:** the inference seam shipped classic-ML shapes first and fixed the LLM direction without building it. The motivating workload is the dominant *backend* LLM use: extraction, classification, scoring, routing, single-shot completion — typed `In → Out` where the model happens to be an LLM. A 2026-09-14 review asked whether that is worth building at all, given that the provider SDKs exist and are better than anything here could be. It is, for one reason that has nothing to do with protocol coverage (§1) — and at roughly a quarter of the original scope (§7).

---

## 1. The argument, stated honestly

**This does not compete with the provider SDKs, and must never try.** They will always have better
coverage of streaming, tool calls, prompt caching, reasoning parameters, files and vision, because
they ship with the provider. An adapter chasing that surface is a treadmill, and the 2026-07 draft
was on it: two clients, one of them a product SDK, against the fastest-moving APIs in the stack.

The thing worth having is narrower. In a forze app the model call is currently the **only** call
that escapes the plane:

- it is not bounded by the enclosing operation's remaining deadline,
- it carries no per-tenant route or credential resolution,
- it declares no egress, though it is the most egress-shaped call an app makes,
- it has no resilience policy beyond what the caller hand-writes,
- and it **cannot be simulated** — so an agent flow is untestable exactly where it is most concurrent.

That last one is the load-bearing one. [RFC 0023](0023-operation-tool-bridge.md) proved an agent's
*tool calls* hold a declared invariant under interleaving; the model call in the same loop is a hole
in that proof. `MockInferenceAdapter` already exists, so the simulated half of the seam is built and
tested — what is missing is only the real half.

It is also the same argument [RFC 0051](../logs/T-0051.md) settled for HTTP three units earlier: a
token endpoint should not have to be called from a bare client, outside the plane's tenancy,
deadlines and egress declaration. Substitute "a model endpoint".

**When not to use this.** An app that owns a multi-turn agent loop — tool calls, conversation state,
streaming to a user — should use its vendor SDK for the model and RFC 0023's bridge for the tools.
This plane is for *typed one-shot generation inside a governed operation*, which is the workload the
seam was built for and the one an SDK gives no governance for.

## 2. Tier 0 — one more dialect

### 2.1 Where it lands

`forze_inference.http` is already a protocol-strategy plane: a `WireProtocol` encodes a batch of
validated instances into one request and decodes the response into per-instance records, and
`HttpInferenceConfig.protocol` selects the dialect while the kernel client owns tenant routing,
credentials, pooling and error-taxonomy translation. Tier 0 is a third value of that field:

| `protocol` | Covers |
| --- | --- |
| `kserve_v2` | KServe, mlserver, Seldon, Triton HTTP |
| `mlflow` | legacy `/invocations` scoring |
| **`openai_chat`** | **anything speaking `/v1/chat/completions`: OpenAI, vLLM, Ollama, LM Studio, OpenRouter, TGI, Groq, Azure, Together — and Anthropic through its OpenAI-compatibility endpoint** |

One dialect, zero provider-specific code, and the broadest coverage available from a single stable
endpoint shape. Everything else on the route — extras, tenancy, `NamedResourceSpec` model ids,
pooling, the egress gate — is reused, not rebuilt.

### 2.2 Prompt-as-config

The route config owns everything prompt-shaped; the handler passes a typed instance and gets a typed
one back, never seeing a prompt, a model id or a provider:

```python
INVOICE_EXTRACTOR = InferenceSpec(name="invoice_extractor", input=DocumentText, output=InvoiceFields)

# wiring (composition root) — the SQL-as-config move, applied to prompts
HttpInferenceConfig(
    protocol="openai_chat",
    model_name="gpt-5",
    prompt=PromptTemplate(
        system="You extract invoice fields precisely. Unknown fields are null.",
        template="Extract the invoice fields from this document:\n\n{text}",
    ),
    acknowledge_data_egress=True,
)
```

- `PromptTemplate` is a nested value object, **required iff** `protocol="openai_chat"` and refused
  otherwise — fail-closed in both directions, so a prompt on a KServe route is a boot error rather
  than a silently ignored field.
- `template` slots are `str.format` names bound from `instance.model_dump()`. The existing
  `validate_against_spec` hook carries the check: every slot must name an input field, and at least
  one slot must exist — a typo'd slot is a `configuration` error at resolve, not a malformed prompt
  in production.
- `output_mode: "structured" | "text"` (default `structured`): structured derives the JSON schema
  from `spec.output.model_json_schema()` and sends it as `response_format` json_schema, failing
  closed at wiring on schema features the constraint rejects, with the offending fields named.
  `text` requires the one-field-`str` output model (the wrap-scalars rule) and fills it.
- Sampling and effort parameters are **config**, never per-call options; `InferenceRunOptions` stays
  timeout-only here, so a model's behaviour is a reviewed wiring fact.

### 2.3 Semantics under the locked contract

- `predict` = one completion. `predict_many` = N sequential requests, **all-or-nothing** per the
  contract; `predict_stream` chunks and honours `max_batch_size` as the other remote adapters do.
- **Capabilities:** `native_batch=False`, `supports_stream=True`, `deterministic=False` (config may
  claim it for seeded or greedy local servers).
- **Usage is telemetry.** Token counts land on the port-instrumentation span as attributes; prompt
  capture stays redacted by default. No envelope method.
- **Error taxonomy**, mapped in the kernel client as the plane already does: rate limit →
  `throttled`/`inference_throttled`; auth or unknown model → `configuration`/`inference_route_mismatch`;
  5xx or overload → `infrastructure`/`inference_endpoint_unavailable`; budget expiry →
  `timeout`/`inference_timeout`; a structured response that fails to validate →
  `validation`/`inference_output_mismatch`. **One new code:** a provider safety refusal is
  caller-content-caused rather than a wire defect → `precondition` / `inference_content_refused`,
  non-retryable. Additive to the seam's error table.

## 3. Non-goals (restated hard)

No agent loops, no conversation or session state, no tool-call execution, no prompt DSL beyond
`str.format` slots, no raw-messages passthrough (multi-turn state is app data), no chat vocabulary
until a second dialect *forces* shared VOs, and no `contracts/llm` — every piece lands in
`contracts/inference` or `forze_inference.http`. **And no second provider client**: a native
Anthropic or OpenAI SDK adapter ships only if the compatibility endpoint provably fails a named
consumer (§8 rows 3, 22), because a second client doubles the maintenance surface for coverage the first
one already has.

## 4. Tier 1 — token streaming (specified, demand-gated)

Delta streaming streams *within one prediction* — a different signature from `predict_stream`'s
instance chunks, and per the seam's decision #2 it can never be added to `InferencePort`. It arrives,
when a consumer asks, as an additive sibling port in the same area:

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GenerationDelta:
    text: str = ""            # incremental completion text (may be empty on the terminal delta)
    done: bool = False        # terminal marker; exactly one per generation

class GenerationStreamPort[In: BaseModel](BaseInferencePort, Protocol):
    def generate_stream(
        self, instance: In, *, options: InferenceRunOptions | None = None,
    ) -> AsyncGenerator[GenerationDelta]: ...
```

- Dep key `generation_query` (read-plane), accessor `ctx.inference.generation(spec)`, capability-gated
  by an additive `token_stream` flag; classic-ML adapters simply never claim it.
- **Text mode only in v1** — structured outputs do not stream usefully token-by-token. The assembled
  final `Out` and structured-mode streaming wait for a real consumer; an additive field or a terminal
  variant can carry them later without a break.
- **Realtime composition is the point**: a handler consumes deltas and publishes `RealtimeSignal`s
  through the shipped egress plane, so the seam grows no transport.
- **Mock**: the route's registry function returns the full text and `MockGenerationStream` slices it
  into fixed-size deltas — deterministic under DST replay.

## 5. Phases

| Phase | Deliverable |
| --- | --- |
| **P1** | `OpenAiChatProtocol` + `PromptTemplate` + the wiring checks (slots ⊆ input fields, schema-feature refusal, prompt-protocol pairing) + the refusal taxonomy row + unit tests over `httpx.MockTransport` + a simulated leg through `MockInferenceAdapter` + a docs section and recipe |
| **P2** | `AnthropicMessagesProtocol` (`protocol="anthropic_messages"`) over the native `/v1/messages`: structured outputs through `output_config.format`, the per-dialect schema-refusal split (row 23), a refusal read off `stop_reason` (row 24), unit tests over `httpx.MockTransport` shaped from the published field tables, and the docs' Anthropic paragraph rewritten. A third dialect on the shipped kernel client — no SDK, no client, no extra. Re-scoped 2026-09-16, row 22 |
| P3 *(demand-gated)* | `GenerationDelta` + `GenerationStreamPort` + `token_stream` capability + `ctx.inference.generation`; protocol and mock streaming; realtime-egress recipe |
| P4 *(deferred)* | Provider batch APIs vs [RFC 0004](0004-batch-inference-plane.md)'s ports — both providers' batch surfaces are inline-requests-plus-poll, a different shape from storage-location ports; investigate before mapping either way |
| P5 *(demand-gated)* | A native provider client — the phase P2 used to be — only against a named consumer that no *dialect* can serve. What the compatibility endpoint failed at is reachable over the wire, so this now needs a gap in the provider's own HTTP API rather than in one endpoint of it |

P1 and P2 add no extra and no package: both dialects ride `inference-http`, and the kernel client
already takes arbitrary default headers, so `x-api-key` and `anthropic-version` are wiring rather
than client code. Only P5 would bring a new extra, with the usual registration mechanics
(pyproject, import-linter, vulture/deptry, changelog, docs).

## 6. Acceptance

P1 is done when, and only when:

1. A route declared with `protocol="openai_chat"` answers `ctx.inference.model(spec).predict(...)` with
   a validated `Out`, against `httpx.MockTransport` standing in for the server.
2. Each wiring mistake is a boot error naming the offender: a slot no input field provides, a prompt on
   a non-chat protocol, a chat protocol with no prompt, an output schema the constraint rejects.
3. Each taxonomy mapping in §2.3 is asserted from a real provider-shaped response body, including the
   refusal → `inference_content_refused` row.
4. A simulation drives an operation whose handler calls the seam, through `MockInferenceAdapter`, and
   the run reproduces from its seed — the hole in §1 is closed, and demonstrably so.
5. The docs section says plainly when to use the vendor SDK instead (§1, last paragraph).

P2 is done when:

1. A route declared `protocol="anthropic_messages"` answers `ctx.inference.model(spec).predict(...)`
   with a validated `Out` in **structured** mode, against `httpx.MockTransport`.
2. The refusal set is the dialect's own, asserted in both directions from one output model: a field
   the chat dialect refuses for a keyword Anthropic accepts — a string `format`, an `anyOf`, a
   defaulted field — wires clean here and is still refused there.
3. A `stop_reason` of `refusal` maps to `precondition` / `inference_content_refused`, and one of
   `max_tokens` is refused as a truncated completion, as the chat dialect already does.
4. The docs' Anthropic paragraph names the dialect instead of the text-mode-only limitation, and
   still says where a vendor SDK is the better tool.

## 7. What the 2026-07 draft said, and what changed

The original Tier 0 was **two new submodules** — `forze_inference.openai_compat` with its own httpx
kernel and `forze_inference.anthropic` over the official SDK — plus a dependency-free
`forze_inference/llm/` sibling for shared glue, with the Anthropic adapter as P2 and streaming as P3.

Three things moved:

- **The plane grew the seam it needed.** `HttpInferenceConfig.protocol` and the `WireProtocol`
  strategy now exist with two implementations, and the kernel client already carries tenancy,
  credentials, pooling and taxonomy. A separate client would rebuild all of it; a dialect reuses it.
  This is the substance of the rewrite, and it takes P1 from a package to roughly 300 lines.
- **The second adapter lost its case.** Anthropic exposes an OpenAI-compatibility endpoint, so a
  native SDK adapter buys little coverage and doubles exposure to a fast-moving API. It becomes
  demand-gated (row 3), and row 13 records that this rests on an API fact to verify at execution.
- **The value argument was restated.** The draft's case was provider-blindness and wiring-level
  swaps. That is real but not sufficient — §1 now leads with the simulability hole RFC 0023 exposed,
  and says outright where a vendor SDK is the better tool.

Everything else the draft established is carried forward: prompt-as-config and its fail-closed
checks, the structured/text split, params-as-config, usage-as-telemetry, the refusal code, the
capability values, the `GenerationStreamPort` shape, and the hard non-goals.

The draft had never been executed and nothing pointed at its decision table, so it is rewritten
rather than amended — an append-only table plus three supersessions would have left the real design
to be reconstructed from the diff. The table below is renumbered and, for the first time here, graded.

## 8. Decisions

*Rows 1–13 are the design's. Rows 14–21 were proposed by execution and each cites the log entry
that produced it; row 13 is superseded by the first of them. Rows 22–25 are the design's again —
P2 re-scoped against what row 14 turned up. The table is append-only: a superseded row stays,
because it is the record that the design once believed otherwise.*

| # | Decision | Grade |
| --- | --- | --- |
| 1 | Tier 0 is adapter-only; prompt-as-config with fail-closed slot and schema validation at wiring | `LOCKED` |
| 2 | Tier 0 ships as one `WireProtocol` (`protocol="openai_chat"`) inside `forze_inference.http` — no new submodule, no new client, no new extra | `LOCKED` |
| 3 | ~~No second provider client until a named consumer proves the compatibility endpoint insufficient; the first such failure is what opens P2~~ — **superseded by row 22**: what the endpoint failed at is a dialect's job, and the gate is narrowed to the client it always named | `LOCKED` |
| 4 | Prompt configuration is a nested `PromptTemplate`, required iff the protocol is `openai_chat` and refused otherwise | `ASSUMED` |
| 5 | Structured mode derives the provider constraint from `spec.output`'s JSON schema; text mode requires the one-field-`str` output model | `LOCKED` |
| 6 | Sampling, effort and model parameters are config, never per-call options | `LOCKED` |
| 7 | Usage and cost are telemetry (span attributes); no envelope method | `LOCKED` |
| 8 | A safety refusal maps to `precondition` / `inference_content_refused`, non-retryable | `LOCKED` |
| 9 | `predict_many` is N sequential requests, all-or-nothing, `native_batch=False` | `ASSUMED` |
| 10 | Token streaming is a `GenerationStreamPort` sibling (dep key `generation_query`, `token_stream` capability), text-mode-only in v1, deterministic mock chunking — demand-gated | `LOCKED` |
| 11 | Chat vocabulary, provider batch mapping and structured-mode streaming are all demand-gated | `LOCKED` |
| 12 | The plane serves typed one-shot generation inside a governed operation; an app owning a multi-turn loop uses its vendor SDK for the model and RFC 0023's bridge for the tools, and the docs say so | `LOCKED` |
| 13 | ~~Anthropic coverage is assumed via its OpenAI-compatibility endpoint, including structured outputs. Verify at execution; if it does not hold, row 3's gate is what fires~~ — **superseded by row 14**: the verification ran and the structured half does not hold | `ASSUMED` |
| 14 | Anthropic's OpenAI-compatibility endpoint is covered in **`text` mode only**. It *ignores* `response_format` rather than rejecting it, so a structured route there answers with prose the output codec then refuses as `inference_output_mismatch` — a wiring mistake that surfaces as a per-request output failure, which is why the docs do not list it as covered. Row 3's gate is unchanged: a native client still waits for a named consumer, and this is not one. Added by execution 2026-09-16 — see `logs/T-0006.md` (D-13, departed) | `ASSUMED` |
| 15 | A `WireProtocol` declares `instances_per_request` (`None` = a whole batch); the adapter derives `native_batch` from it and fans a batch out into that many wire calls. Special-casing the chat dialect inside the adapter was rejected: it would leave `native_batch=True` lying for any future single-instance dialect, and that capability is what the in-memory oracle mirrors to make a batch gate fail where a deployment would. Added by execution 2026-09-16 — see `logs/T-0006.md` (Unlisted, 16:12Z) | `ASSUMED` |
| 16 | Structured mode sends `strict: true` and refuses at wiring every schema feature strict mode rejects, naming the offending fields. Non-strict was rejected: it makes §6's "answers with a validated `Out`" a hope the endpoint does not hold up, while row 1 already locks fail-closed schema validation at wiring. Added by execution 2026-09-16 — see `logs/T-0006.md` (Unlisted, 16:18Z) | `LOCKED` |
| 17 | A refusal is a non-empty `message.refusal` **or** `finish_reason == "content_filter"`, and the provider's wording is withheld from the error. Reading only the field would take an Anthropic refusal (where it is always empty) for an empty completion and fail it as an output mismatch, hiding a caller-content cause behind a wire-defect code. Added by execution 2026-09-16 — see `logs/T-0006.md` (Unlisted, 16:24Z) | `LOCKED` |
| 18 | The derived schema is **tightened** before it is sent — every object gains `additionalProperties: false` — while optionality is **refused**. Closing an object asks nothing of the author and only narrows what the provider may answer with, which is what the declared output model already means; refusing an open model would fail every ordinary Pydantic class for something the plane can fix itself. A defaulted field is refused rather than filled because forcing it changes what the model is asked to do. Added by execution 2026-09-16 — see `logs/T-0006.md` (Unlisted, 16:40Z) | `ASSUMED` |
| 19 | Amends §2.3's "auth" clause rather than the code: the shipped kernel client maps 401/403 to `infrastructure`/`inference_endpoint_unavailable` and only 404 to `configuration`/`inference_route_mismatch`. An upstream auth refusal is a deployment fault — an expired service credential, a WAF rule — and classifying it as a caller error would surface a permanent 422 with no 5xx alert and no retry for something an operator must fix. The dialect adds no mapping of its own. Added by execution 2026-09-16 — see `logs/T-0006.md` (Unlisted, 16:44Z) | `ASSUMED` |
| 20 | Amends §2.2's promise that `validate_against_spec` carries the whole prompt check: only the **value-independent** part is checked at wiring — slots against the input model's fields, and conversions (`!r`, `!z`) for every field type. A **format spec** is not checked at wiring at all, because the declared type does not determine what `str.format` receives (a `Decimal` crosses as a string by default and as a number with a `field_serializer`, same annotation either way); `encode_request` classifies a render failure as `configuration` so the residual failure names the route instead of escaping as a bare `ValueError`. A validation probe that invents its own input is testing itself. Added by execution 2026-09-16 — see `logs/T-0006.md` (Review round 4) | `LOCKED` |
| 21 | A nested format-spec field — `{text:>{width}}` — is **refused at wiring**, not validated. `width` binds from the input instance, which is caller data, and `str.format` sizes an allocation from it: a width of 5,000,000 builds a five-megabyte prompt and 10\*\*10 asks the process for ten gigabytes before any transport limit applies. A prompt is text for a model, not a report column, so a static width is the whole requirement. Added by execution 2026-09-16 — see `logs/T-0006.md` (Review round 7) | `LOCKED` |
| 22 | **A second dialect is not a second client.** P2 becomes `anthropic_messages` — a third `WireProtocol` over the native `/v1/messages`, on the same kernel client, config, taxonomy and adapter — and it is *not* demand-gated. The compatibility endpoint carries no schema constraint at all: it ignores `response_format`, ignores a tool's `strict`, and its own page says "For JSON output, use Structured Outputs with the native Claude API", so typed `In → Out` — the plane's whole promise — is unreachable there for one provider (row 14). Row 3's gate survives for what it named: a native **SDK client** is P5, and now needs a gap in the provider's HTTP API rather than in one endpoint of it. Verified 2026-09-16 against `docs.claude.com/en/api/openai-sdk` | `LOCKED` |
| 23 | Which schema keywords are refused is **the dialect's**, not the plane's. Anthropic's accepted set is strictly wider than OpenAI strict mode's — `anyOf`, `allOf` without `$ref`, `$ref`/`$defs`, `default` on every supported type, string formats (`date-time`, `date`, `email`, `uri`), `enum` — while both require every object closed to extra properties. One shared refusal set could only be the intersection (refusing models Anthropic serves) or the union (sending OpenAI schemas it rejects at request time), so P2 moves the refusal set behind the protocol and P1's checks become the chat dialect's own | `ASSUMED` |
| 24 | The native dialect reads a refusal off `stop_reason == "refusal"` — a first-class value carrying a `RefusalStopDetails.category` from a closed policy vocabulary, rather than the chat dialect's two-shape heuristic (row 17) — and maps it to row 8's code. Whether that category travels anywhere (error detail, span attribute, or nowhere, as the provider's refusal wording is withheld today) is delegated to implementation | `OPEN` |
| 25 | Two request-shape facts are believed rather than verified, and they are row 13's class: that `max_tokens` is required on `/v1/messages`, and that the constraint should be sent as `output_config.format` while the older `output_format` is still accepted. Both are checked at execution against the live reference, and a departure is logged rather than absorbed | `ASSUMED` |
| 26 | The prompt-protocol pairing check keys on the **generation set** (`openai_chat`, `anthropic_messages`), not on one protocol name, and the four generation-only fields are refused against that same set. Keying on one name was rejected: the check would pass a prompt through unread on the second dialect, which is the failure row 4 exists to prevent. Amends row 4, which was stated for the plane as it was when only one dialect could generate. Added by execution 2026-09-17 — see `logs/T-0006-P2.md` (D-4, departed) | `ASSUMED` |
| 27 | An `anthropic_messages` route with **no `max_output_tokens` is refused at wiring**, the way a generation dialect with no prompt already is: `/v1/messages` requires `max_tokens` on every request. A dialect-chosen default was rejected — the plane refuses a truncated completion as an output mismatch, so a default would pick the truncation point for an operator who never saw it and turn a wiring choice into a per-request failure that reads as a model problem. Added by execution 2026-09-17 — see `logs/T-0006-P2.md` (Unlisted, `max_tokens`) | `ASSUMED` |
| 28 | `x-api-key` and `anthropic-version` stay **wiring**, carried by `InferenceHttpSettings.default_headers`, and the docs name them for this dialect. Widening `WireProtocol` with a header channel was rejected: it is a public extension point, and one provider's transport requirement would become every dialect's contract. The cost is stated rather than hidden — a missing `anthropic-version` is a 400 at the first request, not a boot refusal — and a route-config `required_headers` checked at freeze stays open if that trade turns out wrong. Added by execution 2026-09-17 — see `logs/T-0006-P2.md` (Unlisted, headers) | `ASSUMED` |
| 29 | Settles row 24: the refusal's policy category travels **nowhere**. It arrives beside an `explanation` that quotes back a prompt built from the caller's own input, and this plane withholds every upstream body for exactly that reason. A span attribute would be safe from that objection and was still rejected: it would be the only per-provider attribute on a route's span, and nothing on this plane acts on it. The stop reason is what the code carries, and the code is what a caller branches on. Added by execution 2026-09-17 — see `logs/T-0006-P2.md` (D-24, resolved) | `LOCKED` |
| 30 | Two further shapes the constraint cannot express are refused at wiring, in the **shared walk**, for every dialect: a schema whose `additionalProperties` is `true` (a bare `dict`, or a model with `extra="allow"`), and a node carrying none of `type`, `$ref`, `anyOf`, `enum` or `const` (`Any`, `object`, a bare `list`'s member). The first is refused rather than narrowed because row 18's licence to tighten is that closing an object only narrows what the declared output model already means — and for a field declared to take any keys, that is exactly what it does not mean; `tighten` had been rewriting it to `false`, leaving a constraint that permitted only `{}`. This closes the P1 dialect's hole, which is where the shape came from. Added by execution 2026-09-17 — see `logs/T-0006-P2.md` (Unlisted, schema shapes) | `ASSUMED` |
| 31 | A **sampling temperature above the endpoint's ceiling is refused at wiring**, per dialect (2 for chat completions, 1 for Messages). The endpoint rejects the request otherwise, which makes a route that never worked look like a model outage on its first call; the ceiling is a published per-endpoint fact, so it belongs beside the dialect that knows it. Added by execution 2026-09-17 — see `logs/T-0006-P2.md` (Review round 1, finding 5) | `ASSUMED` |
| 32 | `pattern` is **served over a subset** rather than refused outright: the native constraint enforces it, so refusing the keyword would cost a route the constraint it asked for, and what the subset leaves out (word boundaries, lookaround, backreferences) is refused **by name**. Deciding which constructs are active needs a scan that tracks escape parity and character-class boundaries, not a search — three review rounds found the same lexer wrong in three ways, all of them over-refusals. If a fourth arrives, the honest fix is to parse the pattern rather than scan it. Added by execution 2026-09-17 — see `logs/T-0006-P2.md` (Review rounds 5, 7 and 8) | `ASSUMED` |
