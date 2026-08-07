# RFC 0021 — Sandbox execution plane: governed out-of-process code execution

- **Status:** 📝 Draft (demand-gated — ships against a named consumer, not speculatively)
- **Scope:** A plane for **running code out-of-process under governance**: `contracts/sandbox/` (spec, port, request/result value objects, a fail-closed isolation capability model) + a stdlib base subprocess adapter in core integrations (explicitly **non-isolating**, trusted-provenance only) + an in-memory mock with deterministic scripted results and fault injection + a mock-vs-real differential leg. Isolating adapters (container / remote sandbox service) are recorded workstreams, each gated on a named consumer. The plane's entire value is the **fail-closed isolation gate, the DST cut, and the cancel/OOM/output-capture contract** — not the isolation implementation, which the framework can only ever thinly wrap over app-owned infrastructure. §1 argues why that is still worth centralizing, and §2 reconciles the direct tension with the eis-dag conclusion that this is *not* a framework gap.
- **Related:** `run_cpu` (`forze/base/primitives/cpu.py`) is the in-process sibling this plane is defined *against* (§3). The DST loop's `_forbidden("spawning a subprocess")` (`forze_dst/loop.py:267`) and `_forbidden("offloading to a thread/process executor")` (`:272`) are not obstacles but the **specification of where the seam must cut** (§6). The dynamic-read provenance gate (RFC 0015) is the exact security-posture precedent — declare your threat tier, the wiring guard refuses under-isolated adapters for untrusted input. The storage plane is reused wholesale for I/O (§4, the eis-dag "pass by key not path" conclusion). The inference egress-ack flag is the network-policy precedent.
- **Origin:** Two independent analyses converged on the same missing capability from opposite directions. Linecust's G1: the Data Agent needs exploratory compute and recipe validation over generated code, with no framework seam for it. eis-dag's point 2: a plugin runner shelling out to 16.5k LoC of vendored CLI scripts, with hard-kill / OOM-isolation / GIL-escape value the analysis left "app-owned." This RFC finds the shape that serves both without forcing either — and is honest that the framework's contribution is governance and determinism, not a sandbox it cannot ship from a Python library.

---

## 1. The value is the gate and the cut, not the sandbox

State the uncomfortable fact first: **a pure-Python library cannot provide a security sandbox.** Real isolation — a mis-scoped or hostile program that cannot read the host filesystem, cannot reach the network, cannot ptrace a sibling, cannot escape a resource cap — requires a container runtime, a gVisor/nsjail-class supervisor, a microVM, or a remote execution service. None of those ship inside `forze`. So the framework can ship exactly one adapter itself (a bare `asyncio` subprocess), and that adapter is **not a sandbox**: the child shares the kernel, the filesystem, the network, and the user.

Given that, is a framework plane worth it at all, or is this forever app territory? The plane earns its place on three things the framework *can* provide and that are easy to get dangerously wrong per-app:

1. **A fail-closed isolation gate.** The single most valuable line of code here is the wiring-time refusal: *untrusted provenance × non-isolating adapter → boot fails.* Every app that runs generated code will otherwise, eventually, run it in something that isn't a sandbox — the bare-subprocess adapter during a rushed local setup, a mock that got wired in prod. Centralizing the gate (identical to RFC 0015's provenance refusal) turns "someone forgot to containerize the code interpreter" from a silent catastrophe into a failed boot. This is the "built the mechanism, not the gate" audit theme answered *before* it's a finding.
2. **A DST cut.** Out-of-process execution is real, off-loop, wall-clock work — the exact thing DST's loop forbids. Without a framework seam, an app that shells out is un-simulable at that boundary; a Pipeline Engine calling it loses its determinism story. A contract with a deterministic mock lets the app quarantine the probabilistic/real work behind a seam DST can cut — precisely how the inference plane keeps model calls out of simulation (§6).
3. **A cancel / OOM / output contract that's a minefield by hand.** Hard-kill without leaking zombies or file descriptors; OOM/segfault surfacing as a *result* not a worker crash; bounded output capture that doesn't blow memory on a chatty child; workspace cleanup on every exit path. Each is a known footgun; a governed port writes them once.

So the framework ships the **contract + mock + non-isolating base adapter (capability-flagged, trusted-only) + adapter shapes for the isolating tiers**, and the isolating adapters land against named consumers with their infra. The value proposition is deliberately not "forze sandboxes your code" — it is "forze makes running code out-of-process *governed, gated, and simulable*, and refuses to let you do it unsafely by accident."

## 2. Reconciling the eis-dag conclusion (this looked like a non-gap)

The eis-dag analysis (2026-07-30, decision 2) concluded a subprocess seam is **not** a framework gap: it was a legacy-CLI adapter wrapping trusted vendored scripts, and the residual value (hard-kill, OOM isolation, GIL escape) "stays app-owned." That conclusion was correct **for that framing** and this RFC does not overturn it — it separates two things the word "subprocess" conflated:

| | eis-dag framing (correctly rejected) | This RFC |
|---|---|---|
| What runs | *trusted* known binaries the app ships | *untrusted / generated* code |
| Why out-of-process | incidental (that's how the CLI is invoked) | **essential** — isolation is the reason |
| What's valuable | the argv wrangling (app-specific) | the isolation gate + DST cut + cancel/OOM contract (cross-cutting) |
| Right home | an app adapter | a governed plane |

A subprocess seam *to wrap your own trusted CLI* is app code — building a framework abstraction for it would be indirection, exactly as eis-dag said. A governed plane *to execute untrusted code under a fail-closed isolation gate* is a different animal, and it happens to also serve the trusted-CLI case as its weakest tier (`isolation="process"`, for the OOM-isolation and hard-kill eis-dag wanted but left homeless) — so the two needs unify **without forcing eis-dag to adopt it**. The plane is demand-gated; eis-dag can keep its app adapter, and if it wants governed hard-kill it opts in. Nothing about this makes the earlier call wrong; it makes the boundary precise.

It also aligns with this project's own G1 recommendation: path 1 (the Data Agent emits declarative artifacts, so generated code never runs in prod) remains the preferred MVP design. This RFC is path 2 — worth building because the seam has value independent of the MVP (recipe validation, the agent's quarantined exploratory compute, any future code-interpreter feature), and a governed plane beats every app reinventing the dangerous parts. Demand-gated, not speculative.

## 3. Relationship to `run_cpu` — complementary, non-overlapping

`run_cpu` and the sandbox plane are the two halves of "work that isn't the request," and the RFC draws the line hard so neither grows into the other:

| | `run_cpu` (primitive) | sandbox plane (port) |
|---|---|---|
| Boundary | in-process thread | out-of-process child |
| Cancellation | cooperative only — an uncheckpointed thread is *abandoned*, never killed | **hard-kill** (SIGKILL the child/container) |
| Crash isolation | none — a segfault/OOM takes the worker | **isolated** — child crash is a `SandboxResult`, worker survives |
| Trust | your own code | your own *or generated/untrusted* code |
| Output | a return value | captured stdout/stderr + exit code + declared output files |
| Backend variance | none (thread pool / inline) → **a primitive** | subprocess / container / remote → **a port** |

That last row is why one is `forze.base.primitives` and the other is a contract: `run_cpu` has no backend to route or configure and no capabilities to gate, so it is ambient like `TimeSource`; the sandbox has genuine substrate variance, security-critical route config, and a fail-closed capability model, so it is a driven dependency. The docs will carry a two-line decision aid: *need to keep the loop responsive for your own trusted compute → `run_cpu`; need to run code you don't fully trust, or need a hard red button / crash isolation → the sandbox plane.*

## 4. Contract — `contracts/sandbox/`

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxSpec(BaseSpec):
    provenance: Literal["trusted", "untrusted"]   # mandatory, no default — the threat declaration
    capture_command: bool = False                 # DST/trace value capture of argv+payload (masked otherwise)
    description: str | None = None
```

The spec carries only the *portable* threat declaration; everything deployment-shaped (image, resource ceilings, network policy, mounts) lives on the route config (§5), where it is backend-enforced.

```python
class SandboxPort(Protocol):
    def run(self, request: SandboxRequest) -> Awaitable[SandboxResult]: ...
    def run_stream(self, request: SandboxRequest) -> AsyncGenerator[SandboxEvent]: ...
    @property
    def sandbox_capabilities(self) -> SandboxCapabilities: ...
```

`ctx.sandbox.run(spec)` → `SandboxPort`; dep key `sandbox_run`; **command-plane** (a subprocess is an effect — refused in read-only ops, the procedures-plane stance). Resolved via `resolve_configurable(route=spec.name)`, so per-route resilience, OTel, and interceptors apply.

**`SandboxRequest`** — everything crossing into the child, structured:
- `command: tuple[str, ...]` — **argv only, never a shell string** (injection surface; `shell=True` foreclosed, decision 4). A "run this script" convenience is `program: ProgramPayload(interpreter, source)` which the adapter writes to the workspace and invokes by argv — still no shell.
- `input_files: Mapping[str, StorageKey]` — staged from the storage plane into the workspace by relative name (§ the "pass by key not path" rule); the child sees files, forze owns the transfer.
- `output_globs: tuple[str, ...]` — declared artifacts collected from the workspace to storage after exit; **only declared outputs leave** (an undeclared file the child wrote is discarded with the workspace — no accidental exfiltration via the output channel).
- `env: Mapping[str, SecretRef | str]` — secret-resolved at spawn (never in argv, never journaled).
- `stdin: bytes | None`, `timeout: timedelta`, `resources: ResourceRequest | None` (cpu/mem/wall/fd/output-bytes; **clamped to route ceilings, never above**).

**`SandboxResult`** — JSON-trivial *by construction* (durable steps journal it; the produced *files* are storage keys, never inline bytes): `exit_code: int | None`, `outcome: Literal["exited","killed_timeout","killed_oom","killed_cancel","killed_resource","spawn_failed"]`, `stdout: bytes` / `stderr: bytes` (bounded — capped with a truncation flag, never unbounded), `output_files: Mapping[str, StorageKey]`, `duration`, `resource_usage`. A non-zero exit is a **result, not an exception** — the caller decides whether exit 1 is failure (a generated script legitimately reports errors via exit codes); only the framework's own failures (spawn failure, workspace I/O) raise.

**`SandboxCapabilities`** (fail-closed, the `QueryCapabilities` pattern):
- `isolation: Literal["none","process","container","vm"]` — increasing. `none` = bare subprocess (shared everything); `process` = rlimits + fresh workspace + optional uid drop (resource/fault isolation, **not** a security boundary); `container` = namespaced fs/net/pids; `vm` = microVM/gVisor-class.
- `network: Literal["none","egress"]`, `enforces_memory: bool`, `enforces_cpu: bool`, `hard_kill: bool`, `supports_stream: bool`.
- Honesty rule (battery-pinned): a declared capability without a test that exercises it is a lie — no adapter claims `enforces_memory` without a real OOM test on its backend.

## 5. Route config, wiring gates, and the isolation matrix

Config is per-adapter; the *gates* are shared and fail at freeze:

1. **`provenance="untrusted"` requires `isolation >= "container"`** — `sandbox_untrusted_underisolated`. Process isolation is explicitly **not** a security boundary (shared fs/net/`/proc`/ptrace); only container/vm confine a hostile program. This is the RFC 0015 provenance gate transposed: declare the threat, and the framework refuses to run untrusted code in something that can't contain it. Tier C (actively hostile, e.g. end-user code console) is a `vm` + dedicated-topology documentation stance, not a distinct config value — same shape as dynamic read's hostile tier.
2. **`network="egress"` requires `acknowledge_network_egress=True`** — `sandbox_network_egress_unacknowledged`. A networked sandbox is a data-egress surface (generated code can exfiltrate inputs); default `none`, opt-in loud — the inference egress-ack pattern.
3. **Tenancy** — the workspace, staged inputs, collected outputs, and (for routed adapters) the execution backend are tenant-scoped through the storage plane's existing tenancy; a tenant-aware route with no bound tenant fails closed before spawn (`tenant_required`).
4. **Resource ceilings mandatory** — a route must set wall-clock and output-byte ceilings (no unbounded default); memory/cpu ceilings required when the adapter `enforces_*` them, advisory (documented) when it can't.

Isolation matrix (what ships, when):

| Tier | Adapter | Isolation | Ships |
|---|---|---|---|
| Base | `SubprocessSandbox` (core integrations, stdlib `asyncio`) | `none` / `process` (rlimits + workspace + uid drop) | **this RFC** — trusted provenance only, gate-refused for untrusted |
| Container | `forze_sandbox_container` (Docker/Podman/containerd) | `container` | recorded — trigger: a consumer running untrusted code on owned container infra |
| Remote | `forze_sandbox_remote` (E2B/Modal/Fly-machines/a house service) | `container`/`vm` | recorded — trigger: a consumer wanting managed sandboxing; the adapter is a thin control-plane client, the isolation is the service's |

## 6. DST — the seam is the cut, and the loop already proves it

Under simulation the sandbox is mocked; real execution is **outside the simulation boundary by definition** — it is the exact off-loop wall-clock work `forze_dst/loop.py:267` (`_forbidden("spawning a subprocess")`) and `:272` (executor offload) refuse. That refusal is not an obstacle to route around; it is the specification: *this work must be mocked, and here is where.* If a real subprocess adapter is (mis)wired under DST, the loop fails loud — the correct outcome, telling the author to bind the mock, exactly as it does for threads today.

`MockSandbox` runs a registered `handler: Callable[[SandboxRequest], SandboxResult]` per route (the mock-inference / mock-procedure precedent) — deterministic, no I/O, DST-legal — and the handler can return any `outcome`, so the simulator drives the hard cases the real world rarely produces on cue: `killed_oom`, `killed_timeout`, `killed_cancel` mid-run, `spawn_failed`, non-zero exit, truncated output. Fault rules can force these at a chosen call. The doctrine, stated as the inference plane states its own: **conformance tests contract semantics (cancellation lands `killed_cancel`, outputs collected to storage, tenancy honored, caps enforced), never the executed program's behavior** — the framework guarantees the *seam*, not what runs inside it.

## 7. Cancellation and the honest kill boundary

This is the one place the plane delivers what `run_cpu` structurally cannot, so it must be exactly honest about how far it goes:

- Invocation-deadline expiry or outer cancellation → the adapter **hard-kills** the child (SIGTERM, then SIGKILL after a grace period) and returns `killed_cancel`/`killed_timeout`. This is a real red button — because the work is out-of-process. The eis-dag "true kill exists only at process/container granularity" conclusion is precisely why this plane can offer it and `run_cpu` can't.
- **But the kill's completeness is the adapter's isolation tier.** `SubprocessSandbox` kills the process; a child that double-forked or spawned its own children can orphan them (mitigated by a process-group kill / `PR_SET_PDEATHSIG` on Linux, documented, not universally guaranteed). Only `container`/`vm` tiers guarantee that killing the sandbox reaps *everything* it spawned. The docstring says this plainly: hard-kill completeness scales with isolation tier; if you need "and everything it started dies too," you need containment, not a bare subprocess.
- File descriptors, the workspace, and staged temp files are cleaned on **every** exit path (success, non-zero, kill, spawn failure, cancel) — leak-guard tested, because a sandbox that leaks fds/temp dirs under repeated kills degrades the host, which is the failure mode that matters at scale.

## 8. Acceptance battery ("reading isn't proof" — kill/isolation/cleanup logic is exactly where reads deceive)

1. Round-trip: argv `run` with staged `input_files` and declared `output_globs` → outputs land in storage by key; undeclared workspace files are discarded (no leak channel). *(mock ≡ real subprocess)*
2. Non-zero exit is a `SandboxResult` (`outcome="exited"`, `exit_code≠0`), not an exception; the caller's failure policy is its own. *(mock ≡ real)*
3. Timeout → hard-kill → `killed_timeout`; child is gone (pid reaped), workspace cleaned, fds not leaked across 100 repetitions. *(real subprocess)*
4. Outer cancellation mid-run → SIGTERM→SIGKILL grace path → `killed_cancel`; same cleanup guarantees. *(real subprocess)*
5. Process-group kill: a child that spawns grandchildren — process tier reaps the group where supported, and the docstring's orphan caveat is pinned as a documented-limitation test where it can't. *(real, Linux leg)*
6. Output caps: a child spewing to stdout past the byte ceiling is truncated with the flag set, memory bounded — never OOMs the *worker*. *(real)*
7. Resource enforcement: `process` tier rlimit-caps memory → child OOMs → `killed_oom` as a result, worker survives; `enforces_memory` is only *declared* by adapters that pass this. *(real)*
8. Wiring gates: `untrusted` + `isolation="none"/"process"` fails at freeze; `network="egress"` without ack fails; missing wall/output ceilings fail. *(unit)*
9. Secrets in `env` never appear in argv, journals, traces, or the `SandboxResult`; masked unless `capture_command`. *(unit + DST)*
10. Tenancy: staged inputs/outputs and workspace are tenant-scoped; no-bound-tenant on a tenant-aware route refuses before spawn. *(mock ≡ real)*
11. DST: scripted `killed_oom`/`killed_cancel`/`spawn_failed` under forced schedules; a real subprocess adapter mis-wired under simulation trips `_forbidden` (the fail-loud is the feature). *(DST)*
12. Differential discipline: the mock's outcomes match the real subprocess adapter's for every non-isolation-dependent case; isolation-dependent cases (`killed_oom`, group-kill) are real-leg-only and the mock does not pretend to enforce them.

## 9. Phases

- **P1** — `contracts/sandbox/` + `SubprocessSandbox` (`none`/`process`, argv, storage-staged I/O, hard-kill, bounded capture, cleanup) + `MockSandbox` + gates + battery 1–4, 6, 8–11. Ships against the first named consumer.
- **P2** — `process`-tier resource enforcement (rlimits, uid drop, process-group kill) + `run_stream` + battery 5, 7, 12.
- **P3** — `forze_sandbox_container` against a named untrusted-code consumer (this is what unlocks `provenance="untrusted"` in practice); remote adapter recorded with its trigger.

## 10. Decision log

| # | Decision | State |
|---|---|---|
| 1 | The plane's value is the isolation **gate** + DST **cut** + cancel/OOM/output **contract** — not isolation impl, which the framework can only thinly wrap over app infra; ship contract + mock + non-isolating base adapter + demand-gated isolating adapters | locked |
| 2 | Reconciles, not overturns, eis-dag decision 2: trusted-CLI subprocess = app adapter (correctly rejected); governed untrusted-code execution = this plane; they unify at the `process` tier without forcing adoption | locked |
| 3 | A port, not a `run_cpu`-style primitive — genuine substrate variance (subprocess/container/remote) + security config + fail-closed capabilities; command-plane (refused in read-only ops) | locked |
| 4 | **Programs, never callables.** argv only; `shell=True` foreclosed; `program` payload is written to the workspace and invoked by argv. **No pickle-based process pool, ever** — shipping a Python callable across the boundary means deserializing it on the far side, the exact unpickling-is-RCE trust boundary the inference-local doctrine refuses (inverted). An app wanting Python in a subprocess writes the code to a file and runs the interpreter on it (explicit, inspectable, loggable — identical to what the Linecust agent does). A process-pool `CpuExecutor` an app injects into `run_cpu` at its own risk is neither provided nor blessed here | locked |
| 5 | Provenance gate: `untrusted` requires `isolation >= "container"`; `process` is explicitly not a security boundary; hostile = `vm`+dedicated-topology doc stance (RFC 0015 parallel) | locked |
| 6 | I/O by storage key, never host path (eis-dag conclusion 1); only declared `output_globs` leave the workspace; network default `none`, egress ack-gated | locked |
| 7 | `SandboxResult` JSON-trivial (files as keys) for durable journaling; non-zero exit is a result not an exception; the executed program's behavior is never conformance-tested — only the seam | locked |
| 8 | DST mocks the seam; real execution is out-of-boundary by definition; a real adapter under simulation fails loud via the existing `_forbidden` — the cut is where the loop already refuses | locked |
| 9 | Hard-kill completeness scales with isolation tier — stated in the docstring; only container/vm guarantee full reap; base adapter's orphan risk is a documented-limitation test | locked |
| 10 | Demand-gated: ships against a named consumer, not speculatively. Candidates: Linecust recipe-validation / Data-Agent quarantined compute (path-2 fast-follow); any code-interpreter feature | recorded |
| 11 | Naming: `sandbox` is chosen for *intent* (you reach here because you need isolation), with §1 stating plainly the base adapter is not one; `contracts/process` was the honest alternative considered — the base runner is process management, isolation is an adapter capability. Kept `sandbox` because the name should make an author think about containment before running foreign code; the honesty lives in the capability model, not the omission of the word | recorded (08-01, absorbing a parallel duplicate draft) |
