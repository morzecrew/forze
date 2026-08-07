# RFC 0036 — Engine-tier run control: a plane of its own, or a wing of the listing port?

- **Status:** 📝 Draft — **parked (demand-gated), decision made 2026-08-05: Option C, keep `request_cancel` on the admin port.** The question this RFC exists to answer is now answered; nothing is scheduled. The shipped gate already fails closed and reports the boundary honestly, which is the correct state, not a gap. Re-open only on a §5 trigger — and note that §5's original trigger #2 was **checked and withdrawn** (§5.1), which is most of why the answer is C.
- **Scope:** Decide where `request_cancel` lives for engine-backed durable tiers, resolve a conflict between two RFCs that are both "locked" and mutually unbuildable, and pin what Inngest can *actually* honour — which is less, and stranger, than both drafts assumed. Deliberately **not** in scope: building the adapter.
- **Related:** durable run control shipped for the self-hosted tier (executed 2026-08-05) and left its P3 Inngest mapping unbuilt, with the reason recorded in its execution notes — this RFC is that follow-up. RFC 0013 §6 / decision 6 currently claims this ground and **must be reopened** (§4). The self-hosted durable execution tier is what both extend.
- **Origin:** Executing durable run control. The Inngest mapping was not skipped for effort; three structural facts (§1) make the thing the drafts described impossible to build as described, and none of the three are visible from the design documents.

---

## 1. Three facts neither draft had

### 1.1 `DurableRunAdminPort` is self-hosted-shaped

`DurableRunRecord` carries `attempts` — which **is** the fence token, advanced under a row lock on every claim — plus `available_at`, `idempotency_key`, `leased_until` semantics, and now the two cancel stamps. Every one of those is a mechanic of the Postgres `durable_run` table and the lease protocol. Inngest has no fence, no lease, and no attempt-as-fence.

An Inngest implementation of this port would have to **fabricate** them. That is the mock-that-lies bug pointed outward: the framework's recorded rule is that an adapter stricter or looser than its backend makes correct code fail (or wrong code pass) only against that adapter. A port whose contract promises a fence token, implemented over a backend with no fence, is the same defect with a bigger blast radius — a caller who fences a write against it gets no protection and no error.

### 1.2 Cancel is coupled to listing only by accident of plane

The run-control design put `request_cancel` on the admin port for a good reason — the control plane, not the data-plane store — and that reason still holds. But the admin port is *also* the listing port. So the shipped surface says: to stop a run, you must first be able to page `DurableRunRecord`s.

Those are independent capabilities. A hosted engine plausibly has the first and definitely not the second in our record's shape. The plane separation was right; the co-tenancy with `list_runs` was never argued for, it was just where the port already was.

### 1.3 The Inngest Python SDK has no imperative cancel

Verified against the installed SDK, not inferred: `inngest.Client` exposes `send`, `create_function`, `add_middleware`, `set_logger` and internals — there is no `cancel_run`. The SDK's *only* cancellation surface is `server_lib.Cancel`:

```python
class Cancel(_BaseConfig):
    event: str
    if_exp: str | None      # serialized as "if"
    timeout: int | datetime.timedelta | None
```

— declarative `cancel_on`, bound at **function-definition time**. Imperative cancellation is a REST concern, and `forze_inngest` today speaks only the SDK (its kernel holds the SDK client, event key and signing key; there is no HTTP surface of its own).

RFC 0013 decision 6 says Inngest participates via "**the step adapter's** capability surface". There is no such surface, the step adapter has no reach to a *run*, and the capability as shipped lives on the admin port. The decision is not merely underspecified; it names a component that cannot do the job.

## 2. The decision this RFC exists to make

> **Decided 2026-08-05: Option C.** The three sub-sections below are kept as the reasoning, with the two corrections (§2.1) that moved the answer from A to C.

**Option A — a thin control port.** `DurableRunControlPort` with exactly one verb, `request_cancel`, plus the capability. The self-hosted store satisfies it already (it implements both today). `DurableRunAdminPort` keeps `list_runs` and loses `request_cancel`.

- *Cost:* moving a just-shipped public method; a second dep key where there was one. That second key is **not** just a Protocol — it must be claimed in the conformance manifest, registered in `MockDepsModule`, wired into the Postgres module, pass the Mock coverage gate, and be named in `pages/docs` for docs-floors. Five ratchets.
- *Benefit:* the capability and the port line up. `supports_cancel` stops being a proxy for "did you also implement listing", and an engine can honour a stop without fabricating a record it has no fields for.
- *Why that benefit does not cash out (§2.1a):* **A removes one of §1's three blockers and leaves the two larger ones standing.** Inngest still has no imperative cancel in the SDK, and the durable seam still never holds a platform run id. A port split that leaves the only named engine exactly as blocked as before is speculative generality wearing a plane-separation argument.

**Option B — widen the record.** Make the self-hosted fields optional on `DurableRunRecord` and let each engine fill what it has.

- **Rejected**, unless someone shows otherwise. `attempts` is the fence; an optional fence is not a thing. Every field made optional is a field the self-hosted runner must then defensively handle on a path where the answer is always "present" — new branches that exist only to serve a backend that isn't wired. This trades a real, load-bearing invariant for a hypothetical adapter.

**Option C — status quo.** Engine tiers do not participate. The gate fails closed, the docs point at the engine's own controls.

- *Cost:* the asymmetry durable run control exists to complain about, re-introduced one tier over. Tolerable while nobody is asking; not tolerable once somebody is.

**Decision: C. Never B. A only when a §5 trigger fires** — at which point the split is part of the adapter work, done by someone who knows the shape they need instead of by us guessing it a release early.

### 2.1 The two corrections that moved this from A to C

The first draft of this RFC recommended "C now, A at the trigger", and the ordering advice built on it treated the A/C call as time-critical — decide before the next release cut, while the API is still unreleased and the move is free. Both halves were wrong:

**(a) A does not unblock the thing it was proposed to unblock.** See §2's Option A note: one blocker of three. The remaining two are the expensive ones.

**(b) "Expensive later" is much weaker than it looks.** `DurableFunctionRunner.request_cancel(ctx, run_id)` is the documented entry point and is where the capability gate lives; a later split would not move it. So the blast radius of doing A later is **custom adapter implementors only** — a small, knowable set — not application code. "It is free to do right now" is not a reason to do it; that is how speculative surface gets built during a convenient window.

Together these remove the urgency *and* the payoff, which leaves only the conceptual tidiness of a port that lines up with its capability. Real, but not worth five ratchets and a second dep key with no caller.

## 3. What Inngest could actually honour

Both mappings are real; neither is a parity note.

### 3.1 REST mapping

A signing-key-authenticated call to Inngest's cancellation API. Requires: a new HTTP surface in `forze_inngest` (it has none); the platform run id, which the framework does not hold for engine-run functions — the durable-function seam knows a *spec name* and an event, not a run; and the exact endpoint, auth scheme and idempotency semantics pinned against current Inngest documentation. **Open question — not verified in this RFC; verify before scheduling.** Gives an honest `-> bool`.

### 3.2 Event mapping

`request_cancel` sends an event matching a `cancel_on` the function declared up front. No new transport — it rides `send`, which the adapter already has. But:

- it only works for functions whose author opted in at definition time;
- the ask is fire-and-forget: the return value would mean "the event was accepted", not "a run was found and stopped", so `-> bool` becomes a **lie** against the contract's documented meaning;
- it inverts ownership — the *function author*, not the operator, decides whether a run can be stopped at all.

### 3.3 The consequence for the capability

Under 3.2, `supports_cancel` is **not a per-package constant**. It is per *function*, because it depends on whether that function declared `cancel_on`. The shipped capability surface (`DurableRunControlAware.control_capabilities()`, port-level) cannot express that. Either the mapping is REST-only, or the capability grows a per-spec dimension.

This alone is enough to reopen RFC 0013 decision 6: "boundary discipline + `supports_cancel` participation, nothing more" describes a one-line flip, and there is no one-line flip here.

## 4. What changes in the two sibling RFCs

- **Durable run control** — its execution note on the unbuilt Inngest mapping stands as written. Its named follow-up is this document.
- **RFC 0013 decision 6 — reopen.** Current text: *"Inngest parity = boundary discipline + `supports_cancel` participation, nothing more."* It is unbuildable: the capability is not on the step adapter, the step adapter has no run reach, and the SDK has no imperative cancel. Proposed replacement:

  > Inngest parity = boundary discipline. Run **cancellation** is deferred to RFC 0036: the SDK offers only declarative `cancel_on`, so participation requires either a new REST surface or a per-function capability, neither of which is a parity note.

  Its P4 loses the Inngest cancel item and keeps the docs sweep.

## 5. Demand gate — what would make this worth building

Any one of these, and not before:

1. An application running durable functions **on Inngest** that needs an operator Stop button — the eis-dag-shaped consumer that motivated durable run control in the first place, but on the engine tier.
2. ~~A second engine-backed durable tier landing (RFC 0031's pipeline plane already carries a cooperative-cancel contract) — two consumers make the thin port in Option A a shared seam rather than a one-off.~~ **Withdrawn 2026-08-05 — see §5.1.** A genuine second consumer still counts; 0031 is not one.
3. Evidence that the fail-closed gate is being *worked around* — an adopter hand-rolling cancellation beside the framework is the signal the contract is missing something real.

Absent those, the correct state is the shipped one: the capability says no, the runner refuses, and the docs name the engine's own controls. That is not a gap; it is an honest report of a boundary.

### 5.1 Why RFC 0031 does not count as the second consumer

It was cited as one — in this RFC's first draft and in the ordering advice built on it — on the strength of its index line about carrying "cooperative cancel (run-control doctrine)". Reading the RFC rather than the summary settles it the other way:

```python
class PipelineRunPort(PipelineObservePort[P], Protocol[P]):
    def cancel(self, handle: RunHandle) -> Awaitable[None]: ...
```

Its own port, its own `RunHandle`, its own status enum, and a different signature (`cancel(handle) -> None`, not `request_cancel(run_id) -> bool`). Its §5 says outright that this is *"the same cooperative-cancellation doctrine durable run control argued for the durable tier"* — it reuses the **doctrine**, deliberately and with the consistency called out as intentional. It does not reuse, and has no use for, a shared control port.

The general lesson, worth more than the specific correction: **a plane that borrows a doctrine is not a consumer of a port.** Cross-RFC "this one already does X" claims sourced from an index line are how a shared abstraction gets justified by consumers that were never going to call it.

## 6. Proof obligations (if built)

1. The thin port's capability is checked before the verb, and a non-supporting backend is refused at the gate rather than silently accepted (already proven for the self-hosted tier; must hold per-engine).
2. A cancelled engine run reaches a terminal state observable through whatever read surface that tier *does* have — no "asked and vanished".
3. If the event mapping ships: a function without `cancel_on` reports `supports_cancel=False` and is refused, and a function with it is accepted — the per-function dimension is enforced, not documented.
4. Moving `request_cancel` (Option A) leaves the self-hosted battery green unchanged, including the fence races — the plane split must be a pure refactor for the tier that already works.

## 7. Decision log

| # | Decision | State |
|---|---|---|
| 1 | `DurableRunRecord` is self-hosted-shaped and stays that way; no engine adapter fabricates a fence | locked |
| 2 | Option B (optional fields on the record) rejected — an optional fence token is not a thing | locked |
| 3 | **Option C — `request_cancel` stays on `DurableRunAdminPort`.** A removes one blocker of three and leaves the two expensive ones (§2.1a); a later split reaches custom adapter implementors only, not app code (§2.1b) | **locked (08-05)** |
| 4 | Option A remains the shape *if* a §5 trigger ever fires — as part of that adapter's work, not ahead of it | proposed |
| 5 | RFC 0013 decision 6 reopened; replacement wording in §4 | proposed |
| 6 | Inngest REST endpoint/auth semantics unverified — pin against live docs before scheduling | open |
| 7 | §5 trigger #2 withdrawn: RFC 0031 borrows the cancel *doctrine*, not the port (§5.1) | locked (08-05) |
