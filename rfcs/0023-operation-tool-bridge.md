# RFC 0023 — Operation-tool bridge: forze operations as governed agent tools

- **Status:** 📝 Draft (kits-level, zero contract change; **self-contained** — defines its own tool-dispatch VOs, §2, so it does not depend on the shelved chat port)
- **Scope:** A `forze_kits/integrations/agent_tools/` bridge that (a) **projects** frozen operations into `ToolDef`s (name, description, JSON-Schema input) and (b) **dispatches** a `ToolUse` back through `run_operation` under the live invocation context, returning a `ToolResult` — so an in-process agent loop's tools *are* governed operations: tenancy, permissions, deadlines, resilience, and audit all apply to every tool call, unchanged. This is the Query Agent's backbone ("the LLM selects among governed objects, never writes SQL") and the general answer to "how does an agent do things safely in a forze app." Reuses the operation→schema projection `forze_mcp` and OpenAPI already run; adds the in-process dispatch half.
- **Related:** `forze_mcp` (the symmetry, §1 — external-agent ingress vs in-process-agent tools, same projection). The `FrozenOperationRegistry` + `run_operation` core (dispatch target), the query-discovery projection (`build_query_discovery`, already emitted to OpenAPI and MCP), the tenancy/permission enforcement that `run_operation` already applies. RFC 0022 (the AI-integration doctrine + egress gate — the *other* half of an agentic app: this bridge is how the agent *acts*, 0022's membrane edges are how its calls/egress are *governed*; the two compose but neither depends on the other). The Linecust semantic-layer concept ("agent picks metrics/dimensions/filters; a deterministic engine builds the SQL") — that *is* tool-use over governed query operations.
- **Origin:** An agentic forze app needs a governed way for its agent to *act*. Without this bridge, an app either hand-writes tool schemas (drifting from the operations they mirror) and hand-dispatches them (re-implementing tenancy/permission/validation that `run_operation` already does), or lets the agent reach ungoverned code. The framework already turns operations into agent tools for *external* agents via `forze_mcp`; an in-process loop deserves the same projection pointed inward. Note the boundary this bridge is careful to keep: it turns operations into tools and dispatches them **one at a time** — it is emphatically *not* a loop or an orchestrator (§5), so it carries none of the inference seam's foreclosure risk the chat-semantics question raises.

---

## 1. The symmetry that proves this belongs in the framework

`forze_mcp` already answers "expose my operations to an agent" — for an agent on the *other end of a transport*: it projects operations to MCP tools, verifies the caller's API key, resolves a delegated identity, and dispatches through the registry. This RFC is the **same projection with the transport removed** — an agent running *inside* the app process, in a loop the app owns (its SDK; forze owns no loop — RFC 0022 doctrine), calling the app's own operations:

| | `forze_mcp` (existing) | This bridge |
|---|---|---|
| Agent location | external (MCP client over a transport) | in-process (the app's own loop) |
| Identity | delegated resolver + API-key verify | the live invocation context (already bound) |
| Tool schemas | operations → MCP tool defs | operations → `ToolDef` (§2 VO) |
| Dispatch | transport → `run_operation` | `ToolUse` → `run_operation` |
| Governance | full (tenancy/permissions/deadline) | full — *the same* `run_operation` |

Because both halves already exist for MCP, the bridge is mostly *reuse*: the projection is shared, the dispatch is `run_operation` with a `ToolUse`-to-request adapter. The value is that an in-process agent gets tenancy/permission/validation/audit **for free and identically to every other caller**, instead of an app re-deriving them around a hand-rolled tool table.

## 2. The tool-dispatch VOs (defined here, not inherited)

Provider-neutral, minimal, and deliberately **tool vocabulary, not chat vocabulary** — this is the distinction that keeps the bridge foreclosure-safe: a `ToolDef`/`ToolUse`/`ToolResult` triple describes "run this operation as a tool," which is this bridge's whole job; it says nothing about messages, turns, or conversations (the chat vocabulary the inference seam and RFC 0006 are sensitive about, kept out of the framework by the RFC 0022 doctrine — forze governs the AI membrane's edges and owns no loop or chat surface).

```python
@frozen
class ToolDef:     name: str; description: str; input_schema: JsonDict   # JSON Schema of the op's input
@frozen
class ToolUse:     id: str; name: str; input: JsonDict                   # what the agent asked to run
@frozen
class ToolResult:  tool_use_id: str; content: str | JsonDict; is_error: bool = False
```

The app adapts its SDK's native tool types (`anthropic.types.ToolUseBlock`, an OpenAI `tool_call`, …) to/from these at the loop boundary — a few lines it owns — so the bridge stays provider-neutral and the app's SDK stays the owner of the conversation. Home: `forze_kits/integrations/agent_tools/`, exported for the app's boundary adapter. (If typed agent turns ever genuinely need framework support, they re-enter through the RFC 0006 inference lineage — never a chat plane — and could reuse these VOs then.)

## 3. Projection — operations to `ToolDef`s

```python
def operation_tools(
    registry: FrozenOperationRegistry, *,
    include: OperationSelector,          # explicit allowlist — NOT "every operation" (§5)
    ns: StrKeyNamespace | None = None,
    naming: ToolNaming = default_tool_naming,
) -> tuple[ToolDef, ...]: ...
```

- One `ToolDef` per selected operation: `name` = the operation id (agent-legible, stable), `description` from the operation/spec docstring, `input_schema` from the operation's input model `model_json_schema()` — the **same schema** OpenAPI and MCP already project, so the tool the agent sees can never drift from the operation it invokes.
- Query operations project their **discovery** (`build_query_discovery`: filterable/sortable/aggregatable fields, the DSL shape) into the tool description/schema, so a single `query_documents`-style tool lets the agent express filters/sorts/aggregations in the governed DSL — the Query Agent's "select among governed objects" realized without free SQL. This is the concept doc's semantic-layer thesis, mechanized.
- `sensitive` specs and write-omitted/lenient fields are excluded from projection exactly as they are from route/MCP generation (one exclusion rule, three consumers).

## 4. Dispatch — `ToolUse` to `ToolResult`, through the front door

```python
async def dispatch_tool_use(
    tool_use: ToolUse, *,
    ctx: ExecutionContext,
    tools: OperationToolset,             # the projection + its operation bindings
) -> ToolResult: ...
```

- Validates `tool_use.input` against the operation's input schema **before** dispatch → a malformed agent argument is a `ToolResult(is_error=True)` with the validation detail (the agent can correct on the next turn), **never** a raised exception into the loop and never a half-validated call.
- Dispatches through `run_operation` on the live `ctx` — so tenancy, permission keys, ownership/ABAC, the deadline, resilience, interceptors, and audit **all apply**, identically to an HTTP or MCP call of the same operation. An agent cannot reach an operation the current principal can't; a tool call is a first-class governed invocation, not a side door.
- Maps the outcome to `ToolResult`: success → `content` = the (JSON-projected, codec-encoded) result; a governed failure (`precondition`, `authorization`, `validation`, …) → `is_error=True` with a **caller-safe** message (the error-code, not an internal stack) so the agent gets an actionable, non-leaky signal. Infrastructure failures propagate to the loop (the app decides whether to retry the turn), matching how `run_operation` already classifies.
- **Read vs command tools are distinguished at projection.** A toolset can be marked read-only (only `QUERY`-plane operations project), so a Query-Agent answering questions physically cannot mutate — the write operations are absent from its palette, not merely denied at dispatch. A Data-Agent that must act gets a command-capable toolset, still permission-gated per operation. This is the "an agent's tools *are* its capabilities, and capabilities are wired" line, enforced at the palette.

## 5. The allowlist is mandatory — no "expose everything"

`include` is required and there is no "project the whole registry" convenience, by design: an agent's tool palette is a **security and cost surface** (every tool is a capability the model can invoke, and a bloated palette degrades tool-selection accuracy). Curation is the app's decision, made explicit at wiring — the same posture as the dynamic-read provenance gate and the sandbox isolation gate: the dangerous default (expose all) simply isn't spellable. A toolset is reviewed like a permission grant, because that is what it is.

## 6. What this is not

- **Not an agent loop** — the loop is app code (owned by the app's SDK; RFC 0022 governs its egress); this bridge only makes operations available *as tools* and dispatches them one at a time.
- **Not a tool *runtime*** — the tools are forze operations; the bridge doesn't execute arbitrary functions, host plugins, or run code (that's RFC 0021's sandbox, a different plane the agent could *also* be given a tool for, explicitly).
- **Not dynamic tool discovery** — the palette is wired from the registry at startup, per the frozen-wiring doctrine (a durable agent-run journaling tool names needs them stable; dynamic palettes break resumability — the same reasoning that keeps discovery out of forze generally).
- **No prompt/tool-description authoring DSL** — descriptions come from docstrings; tuning them is editing the operation's docstring, not a templating layer.

## 7. Acceptance battery

1. Projection fidelity: a projected `ToolDef.input_schema` equals the operation's OpenAPI/MCP schema for the same op (one schema, three consumers — asserted, not assumed). *(unit)*
2. Dispatch governance: a `ToolUse` for an operation the bound principal lacks permission for → `ToolResult(is_error=True)` with the authorization code, **no** execution, no leak of internal detail; the identical op via HTTP is denied identically. *(unit + parity)*
3. Malformed argument: bad `tool_use.input` → validation `ToolResult(is_error=True)`, never a raised exception, never a partial call; the agent can retry. *(unit)*
4. Read-only toolset: a command operation is **absent** from a read-only projection (not merely denied); a command toolset includes it, still permission-gated. *(unit)*
5. Query-DSL tool: an agent-supplied filter/sort/aggregate over a projected query tool runs through the governed DSL and returns results; an out-of-policy field is refused by the existing query allow-set. *(mock ≡ real backend)*
6. Tenancy: a tool dispatch under tenant A cannot read tenant B's data — `run_operation`'s tenancy applies unchanged; cross-tenant attempt fails closed. *(mock ≡ real)*
7. DST: real `dispatch_tool_use` calls run under forced schedules — a tool dispatch is a governed operation, so it is simulable exactly like any other; an app driving it from a loop (its LLM calls mocked in its own SDK/transport) gets the tool half real-but-governed end to end. *(DST)*
8. `forze_mcp` parity: the same operation projected for MCP and for the in-process bridge yields equivalent tool schemas — the symmetry is tested, not asserted. *(unit)*

## 8. Phases

- **P1** — `operation_tools` projection + `dispatch_tool_use` + read/command toolset split + mandatory allowlist + battery 1–4, 6, 8.
- **P2** — query-DSL tool projection (discovery → tool schema) + battery 5; the Query-Agent recipe (governed egress + query tools + realtime-streamed answer).
- **P3** — DST tool-dispatch conformance (battery 7) as a shared example.

## 9. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Kits bridge, zero contract change; reuses the operation→schema projection `forze_mcp`/OpenAPI already run + `run_operation` dispatch | locked |
| 2 | Dispatch goes through `run_operation` on the live ctx — tenancy/permissions/deadline/audit apply identically to any caller; a tool is a first-class governed invocation | locked |
| 3 | Governed failures → `ToolResult(is_error=True)` with caller-safe codes (agent-correctable); infra failures propagate to the app loop | locked |
| 4 | Read-only vs command toolsets — an agent's mutating power is set by which operations project, not just by dispatch-time denial | locked |
| 5 | Allowlist mandatory; no "expose the whole registry" — a toolset is a reviewed capability grant | locked |
| 6 | Not a loop, not a code runtime, not dynamic discovery, no description DSL — the bridge only turns operations into tools and back | locked |
| 7 | Query-DSL tool projection is the Query-Agent backbone (agent picks governed objects; DSL/engine builds the query) — the concept doc's thesis mechanized | locked |
