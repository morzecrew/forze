# RFC 0006 — LLM adapters + token streaming (the standalone LLM block)

- **Status:** 📝 Draft — proposed, not started. Executes the inference seam's LLM direction (decision #18): LLM converges **in-area**; no parallel `contracts/llm`, ever.
- **Scope:** Schema-constrained generation as ordinary inference adapters (**Tier 0** — zero contract change: `forze_inference.openai_compat` for every OpenAI-compatible server incl. vLLM/Ollama/OpenRouter, and `forze_inference.anthropic` over the official SDK), built on **prompt-as-config** (the prompt template is route wiring, exactly like procedure's SQL); plus the one genuine contract gap, **Tier 1** intra-response **token streaming**, as an additive sibling port (`GenerationStreamPort`, dep key `generation_query`) sharing the spec, capabilities, tenancy and egress plumbing — never a method added to `InferencePort`. Agent loops, conversation state, tool orchestration, and prompt DSLs stay app/kits territory permanently.
- **Related:** the inference seam's LLM direction (tiers fixed 2026-07-13) + its locked decisions (all-or-nothing `predict_many`, `Out` model-only, egress-ack, capabilities). Wiring shape — `forze_inference.http` (per-submodule extras, `TenantAwareIntegrationConfig`, `NamedResourceSpec` targets, error-taxonomy translation in the kernel client). Prompt-as-config precedent — [`procedure/specs.py`](../src/forze/application/contracts/procedure/specs.py) (physical binding in wiring, typed params in handlers) and analytics' `validate_against_spec` fail-closed exact-checks (here: template slots ⊆ input fields). Delta delivery to clients — the shipped realtime egress plane ([realtime.md](../pages/docs/data-events/realtime.md)): the handler consumes deltas and publishes `RealtimeSignal`s; the seam grows no websocket. Anthropic API facts current as of 2026-07 (structured outputs = `output_config.format` json_schema; assistant prefill removed on current models — structured outputs is the supported constraint mechanism; typed SDK exceptions; `stop_reason: "refusal"`; Message Batches = inline requests + poll, **not** storage locations).
- **Origin:** the inference seam deliberately shipped classic-ML shapes first and fixed the LLM direction without building it. The motivating workload is the dominant *backend* LLM use: extraction, classification, scoring, routing, single-shot completion — typed `In → Out` where the model happens to be an LLM. The framework's value here is exactly its existing governance: handlers never see prompts, model ids, or providers; swapping GPT ↔ Claude ↔ a local vLLM is a wiring change.

---

## 1. Tier 0 — schema-constrained generation (zero contract change)

### 1.1 Prompt-as-config

The route config owns everything prompt-shaped; the handler passes a typed instance:

```python
INVOICE_EXTRACTOR = InferenceSpec(name="invoice_extractor", input=DocumentText, output=InvoiceFields)

# wiring (composition root) — the SQL-as-config move, applied to prompts
AnthropicInferenceConfig(
    model="claude-opus-4-8",
    system="You extract invoice fields precisely. Unknown fields are null.",
    template="Extract the invoice fields from this document:\n\n{text}",
    acknowledge_data_egress=True,
)
```

- `template` slots are `str.format`-style names bound from `instance.model_dump()`. **Fail-closed wiring check** (`validate_against_spec`): every slot must name an input field, and at least one slot must exist — a typo'd slot is a `configuration` error at resolve, not a malformed prompt at runtime (the tenant-param-referenced precedent).
- `output_mode: "structured" | "text"` (default `structured`): structured derives the JSON schema from `spec.output.model_json_schema()` and passes it as the provider's native constraint (Anthropic `output_config.format` json_schema; OpenAI-compatible `response_format` json_schema). **Wiring fail-closed** on schema features the provider constraint rejects (recursive models, numeric/string bounds where unsupported) with the offending fields named. `text` requires the one-field-`str` output model (the wrap-scalars rule) and fills it with the completion.
- Sampling/effort params (`max_output_tokens`, provider-specific `effort`/`thinking`/`temperature`) are **config**, not per-call options — version/params pinning stays a wiring fact (inference-seam governance; `InferenceRunOptions` stays timeout-only for these adapters).

### 1.2 The two adapters

| Submodule | Extra | Client | Covers |
| --- | --- | --- | --- |
| `forze_inference.openai_compat` | `inference-openai-compat` | **own httpx kernel** (protocol adapter, not a product SDK — the `http` submodule doctrine) | OpenAI, vLLM, Ollama, LM Studio, OpenRouter, TGI — anything speaking `/v1/chat/completions` |
| `forze_inference.anthropic` | `inference-anthropic` (`anthropic` SDK) | official `AsyncAnthropic` | Claude API (structured outputs, adaptive thinking/effort as config passthrough) |

Both follow the established submodule shape (`_compat`, `kernel/`, `adapters/`, `execution/deps` + lifecycle) and both implement `InferencePort` so `ctx.inference.model(spec)` is provider-blind. `openai_compat` ships first — broadest coverage including fully-local serving (which also softens the egress story for on-host vLLM/Ollama; the ack flag still applies uniformly, as in `http`).

Shared LLM glue (template rendering + slot validation, schema-derivation checks, delta VO helpers) lives in a dependency-free `forze_inference/llm/` sibling — importable by both submodules without dragging either's client dependency (the `records.py` precedent).

### 1.3 Semantics under the locked contract

- `predict` = one completion. `predict_many` = adapter-side loop (`native_batch=False`), sequential in v1, **all-or-nothing** per the contract — one failed completion fails the batch (callers wanting partial progress use `predict_stream`, whose chunk boundaries checkpoint). `predict_stream` = chunked loop honoring `max_batch_size` sub-batching as in the remote adapters.
- **Capabilities:** `native_batch=False`, `supports_stream=True`, `deterministic=False` (config may claim it for seeded/greedy local servers), `max_batch_size` config.
- **Usage/cost = telemetry.** Token usage from each response is recorded on the port-instrumentation trace/OTel span (attributes, not return values); prompt/input capture stays redacted by default (the inference seam's redaction rule). An envelope method is still explicitly deferred.
- **Error taxonomy** (mapped in each kernel client, like `http`): provider rate limit → `throttled`/`inference_throttled`; auth/unknown-model → `configuration`/`inference_route_mismatch`; 5xx/overload → `infrastructure`/`inference_endpoint_unavailable`; budget expiry → `timeout`/`inference_timeout`; structured output that fails to parse/validate → `validation`/`inference_output_mismatch`. **New code:** a provider safety refusal (Anthropic `stop_reason: "refusal"` and equivalents) is caller-content-caused, not a wire defect → `exc.precondition(code="inference_content_refused")`, non-retryable — additive to the inference seam's error table.
- **Provider batch APIs are explicitly deferred** (P4): both major providers' batch surfaces are *inline requests + poll + result-set keyed by custom id* — a different shape from RFC 0004's storage-location ports. The mapping (location-less submit variant vs provider-file-backed locations) is investigated when picked up; do not force-fit either way now.

## 2. Tier 1 — token streaming (the one contract addition)

Delta streaming streams *within one prediction* — a different signature from `predict_stream`'s instance chunks, and per the inference seam's decision #2 it can never be added to `InferencePort`. It arrives as an **additive sibling port in the same area**:

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

- Dep key `generation_query` (read-plane; `domain="inference"` inferred), accessor `ctx.inference.generation(spec)`, capability-gated by a new `token_stream` flag (additive to `InferenceCapabilities`; validators fail closed as usual). Registered by the LLM adapters and the mock; classic-ML adapters simply never claim it.
- **Text mode only in v1**: streaming pairs with `output_mode="text"` (structured outputs don't stream usefully token-by-token); the assembled final `Out` (and any structured-mode streaming) is deferred until a real consumer needs it — an additive field on `GenerationDelta` or a terminal-event variant can carry it later without breaking.
- **Realtime composition is the point**: the recipe ships a handler that consumes deltas and publishes `RealtimeSignal`s through the egress plane — client push without the seam growing a transport.
- **Mock**: the route's registry function returns the full text; `MockGenerationStream` slices it into fixed-size deltas — deterministic chunking, exact under DST replay.

## 3. Non-goals (restated hard)

No agent loops, no conversation/session state, no tool-call execution, no prompt DSL beyond `str.format` slots, no chat vocabulary until a second adapter *forces* shared `ChatMessage`/`ToolCall` VOs (apps model their own `In` today), no raw-messages passthrough mode (multi-turn state is app data). And no `contracts/llm` — every piece lands in `contracts/inference` or `forze_inference.*`.

## 4. Phases

| Phase | Deliverable |
| --- | --- |
| P1 | `forze_inference/llm/` shared glue (template render + slot/schema fail-closed validation) + `forze_inference.openai_compat` Tier-0 adapter + unit tests (httpx MockTransport) + docs section |
| P2 | `forze_inference.anthropic` Tier-0 adapter (official SDK; structured outputs; typed-exception → taxonomy translation) |
| P3 | Contracts: `GenerationDelta` + `GenerationStreamPort` + `token_stream` capability + `ctx.inference.generation`; adapter + mock streaming; realtime-egress recipe |
| P4 *(deferred)* | Provider batch APIs vs RFC 0004 ports — investigate, then map or extend |

Registration mechanics per new extra as the inference seam did (pyproject extras `inference-openai-compat` / `inference-anthropic`, import-linter, vulture/deptry, changelog, docs).

## 5. Decision log

| # | Decision |
| --- | --- |
| 1 | Tier 0 is adapter-only; prompt-as-config with fail-closed slot/schema validation at wiring |
| 2 | `openai_compat` = own httpx protocol kernel; `anthropic` = official SDK; `openai_compat` ships first |
| 3 | Structured mode derives the provider constraint from `spec.output`'s JSON schema; text mode requires the one-field-`str` output model |
| 4 | Sampling/effort/model params are config, never per-call options |
| 5 | Usage/cost is telemetry (span/trace attributes); no envelope method |
| 6 | Safety refusals → `precondition` / `inference_content_refused` (new, additive taxonomy row) |
| 7 | Token streaming = `GenerationStreamPort` sibling (dep key `generation_query`, `token_stream` capability), text-mode-only v1, deterministic mock chunking |
| 8 | Chat vocabulary + provider batch mapping + structured-mode streaming all demand-gated |
