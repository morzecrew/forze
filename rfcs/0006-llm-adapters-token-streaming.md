# RFC 0006 — LLM adapters + token streaming (the standalone LLM block)

- **Status:** 📝 Draft — **rewritten 2026-09-14**; the 2026-07 draft is superseded in full and §7 records what changed and why. Tier 0 is now **one wire protocol on the shipped inference-HTTP plane**, not two provider submodules with their own clients. Executes the inference seam's LLM direction (decision #18): LLM converges **in-area**; no parallel `contracts/llm`, ever.
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
consumer (§8 row 3), because a second client doubles the maintenance surface for coverage the first
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
| P2 *(demand-gated)* | A native provider client, only against a named consumer the compatibility endpoint fails |
| P3 *(demand-gated)* | `GenerationDelta` + `GenerationStreamPort` + `token_stream` capability + `ctx.inference.generation`; protocol and mock streaming; realtime-egress recipe |
| P4 *(deferred)* | Provider batch APIs vs [RFC 0004](0004-batch-inference-plane.md)'s ports — both providers' batch surfaces are inline-requests-plus-poll, a different shape from storage-location ports; investigate before mapping either way |

P1 adds no extra and no package: the dialect rides `inference-http`. Only P2 would bring a new extra,
with the usual registration mechanics (pyproject, import-linter, vulture/deptry, changelog, docs).

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

| # | Decision | Grade |
| --- | --- | --- |
| 1 | Tier 0 is adapter-only; prompt-as-config with fail-closed slot and schema validation at wiring | `LOCKED` |
| 2 | Tier 0 ships as one `WireProtocol` (`protocol="openai_chat"`) inside `forze_inference.http` — no new submodule, no new client, no new extra | `LOCKED` |
| 3 | No second provider client until a named consumer proves the compatibility endpoint insufficient; the first such failure is what opens P2 | `LOCKED` |
| 4 | Prompt configuration is a nested `PromptTemplate`, required iff the protocol is `openai_chat` and refused otherwise | `ASSUMED` |
| 5 | Structured mode derives the provider constraint from `spec.output`'s JSON schema; text mode requires the one-field-`str` output model | `LOCKED` |
| 6 | Sampling, effort and model parameters are config, never per-call options | `LOCKED` |
| 7 | Usage and cost are telemetry (span attributes); no envelope method | `LOCKED` |
| 8 | A safety refusal maps to `precondition` / `inference_content_refused`, non-retryable | `LOCKED` |
| 9 | `predict_many` is N sequential requests, all-or-nothing, `native_batch=False` | `ASSUMED` |
| 10 | Token streaming is a `GenerationStreamPort` sibling (dep key `generation_query`, `token_stream` capability), text-mode-only in v1, deterministic mock chunking — demand-gated | `LOCKED` |
| 11 | Chat vocabulary, provider batch mapping and structured-mode streaming are all demand-gated | `LOCKED` |
| 12 | The plane serves typed one-shot generation inside a governed operation; an app owning a multi-turn loop uses its vendor SDK for the model and RFC 0023's bridge for the tools, and the docs say so | `LOCKED` |
| 13 | Anthropic coverage is assumed via its OpenAI-compatibility endpoint, including structured outputs. Verify at execution; if it does not hold, row 3's gate is what fires | `ASSUMED` |
