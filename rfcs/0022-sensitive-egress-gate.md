# RFC 0022 — Sensitive-egress acknowledgement gate (`forze_http`), and the AI-integration doctrine

- **Status:** 📝 Draft. **Supersedes the earlier "governed LLM egress" draft this document replaces, which is dissolved** — see the note below and §3.
- **Scope:** Two things, both *subtractive* — this RFC exists to shrink forze's surface, not grow it. **(1)** A general, optional **sensitive-egress acknowledgement gate** on the existing `forze_http` plane: a fail-closed `acknowledge_data_egress` flag that marks a route as knowingly sending data outside the trust boundary. Not LLM-specific — it guards any sensitive outbound call (an LLM provider is one user of it). **(2)** A recorded **doctrine**: forze ships **no** chat/agent/LLM-named framework surface; an AI app is assembled from general primitives. The "govern the LLM call at the transport" idea from the dissolved draft reduces to *this gate on `forze_http`* plus a documentation recipe — there is no "LLM egress plane."
- **Related:** `forze_http` (host of the gate — it already carries tenant routing, `propagate_deadline`, secret auth, OTel; this adds one flag). the inference seam and RFC 0006 (the *actual* bold LLM seam; §2). RFC 0023 (operation-tool bridge — governed tools, the AI-addressable-operations edge). The remote-inference `acknowledge_data_egress` gate this generalizes out of `HttpInferenceConfig`/`SageMakerInferenceConfig` into the base HTTP plane.
- **Dissolution note:** The prior draft proposed a `LlmEgressConfig` / `ctx.llm_egress` surface. Analysis (2026-08-01) found it was ~entirely `forze_http` in an LLM costume: tenant tag, deadline header, secret auth, and OTel already exist there; the only non-redundant atom was the egress-ack flag, and the only other addition (a per-provider usage parser) is best-effort app-shaped telemetry. Naming a plane after LLMs invented an LLM-shaped concept where there was only "call an external service," which forze already does. So the plane is dissolved: keep the one general atom, make the rest a recipe, and do not blur the surface with an LLM-named seam. The agent-tool bridge (RFC 0023) was already general and is unaffected.

---

## 1. The gate (committed — this is the executable part of the RFC)

`HttpServiceConfig` gains two fields:

```python
egress_sensitive: bool = False        # a data fact: this route sends business/PII/prompt data out of the trust boundary
acknowledge_data_egress: bool = False # an operator act: yes, I know it leaves, and I accept it
```

- **The wiring guard.** Constructing a route with `egress_sensitive=True` and `acknowledge_data_egress=False` **fails at wiring** with `exc.configuration(code="http_egress_unacknowledged")`, naming the route. A route left `egress_sensitive=False` (the default) is unaffected — no gate, no behavior change; existing wiring is untouched. The two-field split is deliberate: *sensitivity* is a property of the data the route carries (the app declares it), *acknowledgement* is a conscious operator decision (the deployment accepts it) — collapsing them into one flag would let "I set the bool" masquerade as "I decided."
- **What it is, honestly.** A **governance/audit marker plus a fail-closed conscious-choice gate — not a DLP firewall.** It does not inspect payloads or block traffic at runtime; it forces the egress to be a declared, reviewed wiring fact, and it tags the per-request OTel span (`forze.egress.sensitive=true`) so sensitive egress is queryable in the observability plane. That is exactly the property the remote-inference configs' `acknowledge_data_egress` already provides for model calls — this lifts it out of `HttpInferenceConfig`/`SageMakerInferenceConfig` into the base HTTP plane so it guards *any* sensitive outbound hop (a provider API, a partner integration, an LLM). The inference configs can later delegate to this one implementation rather than carrying their own copy.
- **Everything else is already on `forze_http`** and needs no new code: tenant tagging (`tenant_aware`), remaining-budget propagation (`propagate_deadline`), secret-resolved auth (`HttpAuthConfig`), per-request OTel. An app running a provider SDK (Claude Agent SDK, etc.) through governed egress passes `forze_http`'s client to the SDK — a **recipe**, not a framework surface.
- **Scope of execution.** Two config fields, one wiring-time validator, one span attribute, a docs line, and the delegation from the inference configs. Small and self-contained; ships whenever the executing session picks it up (no consumer gate — a compliance marker is useful on its first sensitive route, and it is cheap enough that "wait for a second consumer" would be ceremony).
- The one thing that stays app code by design: a token/cost usage parser is provider-shaped and best-effort; an app that wants cost telemetry writes ~10 lines reading its SDK's response. The framework does not model provider usage.

This is the whole framework delta for "AI egress": one general flag. If even that has no consumer beyond the recipe, it parks — a flag is cheap to defer.

## 2. The doctrine: forze governs the membrane, owns no loop

The reason there is no LLM/agent plane is not timidity — it is a positioning choice worth recording so it is enforced in every future review. **forze's AI stance is to govern the membrane between the deterministic system and nondeterministic AI, and to own none of the loop.** The membrane has four edges, each already a forze-shaped surface:

1. **The call** → the inference seam, made LLM-real by RFC 0006 (prompt-as-config, structured output, streaming). An LLM becomes a *declared, typed, provider-blind, DST-mockable operation* — the hexagonal philosophy applied to AI. This is the bold seam; it already exists in design.
2. **The tools** → RFC 0023 + `forze_mcp`. Every governed operation is an AI-addressable capability with tenancy/permissions/audit intact — the AI's *unit of action* is a governed operation, not free code.
3. **The context** → embeddings + vector search + every data plane (RAG is assemblable today).
4. **The lifecycle** → durable + realtime + progress (long AI tasks, streamed output, the terminate-and-resume pause the operation-progress kit ships).

The loop — planning, conversation state, memory, orchestration — stays **outside** the membrane, in the app's SDK. The enforcement question for any proposed AI surface is one line: **would this exist if LLMs didn't?** The inference seam (a typed model call), the tool bridge (governed dispatch), the egress gate (sensitive data leaving), the data/durable/realtime planes — all pass. A messages/turns/planning/memory surface fails, and belongs to the app.

Consequences this locks in, against the three standing worries:
- **Not throwaway** — forze has a crisp AI thesis most frameworks can't state because they blurred into agent-builders.
- **Not blurred** — forze owns edges, never the loop; the one-line test keeps it that way.
- **Not a lock-out** — with the inference seam (LLM-real via 0006) + tool bridge + embeddings + vector + `forze_http` + durable + realtime, an app can build *any* AI feature (RAG, extraction, chat, multi-agent) without forze blocking it. Owning the edges keeps the user free precisely because forze does not own the loop; a framework that shipped its own loop would lock them into it.

The active risk is the opposite of over-building: it is *under-communicating*. If RFC 0006 stays a draft and this doctrine stays implicit, forze looks AI-absent though it is AI-native by design — and that appearance creates pressure to bolt on the very chat surface this RFC refuses. The defense is to execute 0006 and keep this doctrine written down, so "where is the AI?" answers with the membrane, not with a new plane.

## 3. What is dissolved, explicitly

- **No `LlmEgressConfig`, no `ctx.llm_egress`, no "LLM egress plane."** Gone.
- **No chat generation port** (the shelved §5 of the old draft is withdrawn with it — if typed/provider-blind/DST-simulable agent turns are ever genuinely needed, they re-enter through RFC 0006's inference-seam lineage as an additive sibling, not as a chat plane; the door is 0006, not a new surface).
- **Kept:** the one general egress-ack atom (§1) and the doctrine (§2). The "run an agent SDK in a forze app" recipe lives in docs, assembling existing planes.

## 4. Decision log

| # | Decision | State |
|---|---|---|
| 1 | The "govern LLM calls" need reduces to a general `forze_http` egress-ack flag + a docs recipe; no LLM-named plane, no `ctx.llm_egress` | locked (08-01) |
| 2 | The egress-ack gate is **general** (any sensitive outbound hop), lifted out of the inference remote-configs into `forze_http` as two fields (`egress_sensitive` data-fact + `acknowledge_data_egress` operator-act) + a wiring guard (`http_egress_unacknowledged`) + a `forze.egress.sensitive` span tag. **Committed to ship** — no consumer gate (a compliance marker earns its place on the first sensitive route); inference configs later delegate to this one implementation | locked (08-01) |
| 3 | Usage/cost parsing stays app code (provider-shaped, best-effort) — the framework never models provider usage | locked |
| 4 | Doctrine locked: forze governs the four membrane edges (call/tools/context/lifecycle) and owns no loop; the review test is "would this exist if LLMs didn't?" | locked (08-01) |
| 5 | The bold LLM seam is the inference seam made real by RFC 0006 — the strategic move is execute + articulate, not build a new AI surface | locked (08-01) |
| 6 | Chat generation port fully withdrawn; any future re-entry is via 0006's inference lineage, never a chat/agent plane | locked (08-01) |
