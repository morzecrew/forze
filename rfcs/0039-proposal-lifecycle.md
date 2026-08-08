# RFC 0039 — Proposal lifecycle: machine-authored artifacts that stay inert until a human accepts

- **Status:** 📝 Draft — **design open on one question**, which this RFC exists to settle before any code: is this a contract or a kit? A leaning is recorded (§3: kit) with the evidence and with what would overturn it. Independent of every other draft in the tree; nothing gates it and it gates nothing.
- **Scope:** The lifecycle above `contracts/inference` — a machine-authored artifact that is **inert until a human accepts it**: propose → evaluate → accept/reject, with evidence attached and an acceptance policy that has no auto path. Covers where the lifecycle lives (core contract vs `forze_kits`), the status vocabulary, and the composition with the shipped inference seam and document plane. Does **not** cover what any application proposes (mappings, metric definitions, remediations — all app models), and does not model review UI or notification.
- **Related:** [`contracts/inference`](../src/forze/application/contracts/inference/) — `InferenceSpec` (`input`/`output`/`capture_inputs`), `InferencePort.predict`, and `InferenceDeps.model`; the layer this sits above and composes rather than extends. [`forze_kits/aggregates/kit.py`](../src/forze_kits/aggregates/kit.py) — the `registry()` / `facade()` / `domain_events()` / `lifecycle_steps()` emission split and `spec_contributions`. [`progress/inventory.py`](../src/forze_kits/integrations/progress/inventory.py) and [`progress/record.py`](../src/forze_kits/integrations/progress/record.py) — a shipped kit that ships its own `DocumentSpec`, registers it `SpecSource.KIT`, and already models *waiting on a human* as `JobStatus.WAITING`. RFC 0006 makes a model-backed proposer real; RFC 0005 (`ModelResolverPort`, parked awaiting "a registry-driven deployment") plausibly finds its trigger in a proposal loop that pins model versions.
- **Origin:** A lakehouse / semantic-layer design proposing `ProposalPort` as a core contract — *"the strongest candidate in this document for promotion into the core"* — for its mapping-proposal loop. Reviewing it against the tree turned the promotion argument inside out: every property the design cites for core-hood is a property the kit layer already provides, and the port's own enforcement turns out to be illusory (§3). The lifecycle is real and recurring; the placement is the open question.

---

## 1. The question

The lifecycle is not in dispute. `InferenceSpec` names one task with typed input and output and `InferencePort.predict` returns a value; there is no vocabulary for *a value that must not take effect until someone approves it*. That shape recurs — mapping proposals, metric definitions, schema suggestions, drift remediations — and it is domain-agnostic.

What is in dispute is where it belongs, and the framework has a test for that: **a port earns its place when several backends sit behind one contract.** Ask it here and the answer is uncomfortable. The two implementations offered are "backed by an `InferenceSpec`" and "a deterministic rules engine" — and both are *functions in the application*, not external systems. That is one implementation with a parameter, which is the shape a kit takes.

This RFC therefore has one job: answer the placement question with evidence, so that whoever builds it is not re-deriving it, and so that a contract is not added to the core on the strength of the lifecycle being important. Importance is not the test.

## 2. The shape, stated once so both answers describe the same thing

Whatever the placement, the model is:

- **The artifact** is a closed, validatable schema. That constraint is what makes a proposal reviewable rather than arbitrary, and it is the same constraint `InferenceSpec.output` already imposes.
- **Evidence** is attached before a decision is asked for. A proposal without evidence is an opinion; the evidence is facts, never a verdict — the reviewer decides.
- **Acceptance requires a principal.** There is deliberately **no auto path**: a framework that ships one will have applications using it, and the whole value is that the artifact is inert until a human acts. An application wanting automation composes a deterministic proposer with its own handler, visibly, in its own code.
- **Statuses**: drafted → evaluated → accepted / rejected, plus superseded when a newer proposal for the same target is accepted.
- **The rows are documents.** Proposals are state, and the framework already has a plane for state an application owns. This is not a concession to the kit answer — the source design reaches it independently, and it is what makes proposals exportable, reconciled and portable for free.

## 3. Why the leaning is "kit"

Four pieces of evidence, in ascending order of how hard they are to argue with.

**It holds no state, so it has no plane.** [`planes.py`](../src/forze/application/contracts/inventory/planes.py) inventories keys whose route is a spec name *and* which bind a stateful resource; crypto, secrets, saga, hlc and the resilience executor sit outside for exactly this reason. A proposal port binds nothing — the rows are on the `DOCUMENT` plane. The source design agrees and calls it "deliberate", but a contract with no plane, no state and no backend is most of the way to being a function.

**Every operation decomposes into shipped primitives.** `propose` is an inference call or a plain function; `evaluate` is application validation; `accept` and `reject` move a status field on a document. There is no operation left over that needs a seam.

**The kit layer already does exactly this, three times.** `AggregateKit.spec_contributions`, `RealtimeTransport.spec_contributions`, and [`progress_spec_contributions`](../src/forze_kits/integrations/progress/inventory.py) all ship a kit-owned `DocumentSpec`, register it with `SpecSource.KIT`, and hand the application a registry to merge. The progress kit goes further and already models the state this lifecycle is about: `JobStatus.WAITING` is documented as *"paused on something external: a human answer, an upstream job, an approval."* The pattern is not hypothetical, it is load-bearing in shipped code.

**The port's enforcement is illusory — this is the decisive one.** The appeal of `accept(by: Principal)` is that the framework guarantees no acceptance without a principal. It does not. The proposals are documents; an application that wants to flip a status writes the document directly through the document port, which is right there in the same context. A port cannot prevent what a sibling port permits. So the fail-closed property that most justifies core-hood is enforced by *whoever owns the handler* — which is the kit, in either design. Putting it behind a contract buys the appearance of a guarantee and not the guarantee.

The DST claim the source design offers as proof of contract-hood — *"a proposed, evaluated, un-accepted proposal must not affect any downstream state"* — survives the demotion intact. It is a property of the application's operations under simulation, and DST already simulates operations. It needs a test, not a port.

## 4. What would overturn the leaning

Recorded so the answer can be revisited on evidence rather than on enthusiasm. Any one of these makes the contract the right call:

1. **A second implementation that is an external system.** Proposals living in a review queue the application does not own — a labelling tool, a data-catalog suggestion inbox, a ticketing system — genuinely put two backends behind one vocabulary, and that is what a port is for. Today there are zero.
2. **A cross-cutting invariant the framework can enforce and a kit cannot.** §3's fourth point says the obvious candidate fails. A different one may exist; it would have to be something the framework enforces *at a boundary the application cannot route around*.
3. **A second application in-tree.** The two-implementations rule applies to kits too. One consumer is a design; two are a pattern.

## 5. If the answer is kit — the shape

`forze_kits/integrations/proposal/`, following the progress kit's split:

- `record.py` — the `ProposalDocumentSpec` factory and the status enum, so applications do not each invent one.
- `inventory.py` — `proposal_spec_contributions(...)`, same contract as its three siblings, refusing an empty contribution the way `progress_spec_contributions` does.
- `operations.py` / `handlers.py` — propose / evaluate / accept / reject as operations over the document, with the principal check in the accept handler where it can actually be enforced.
- `factories.py` — takes an `InferenceSpec` **or** a plain callable; the symmetry is the point, and it proves the lifecycle does not presuppose a model.

Build against a deterministic proposer first and bind an `InferenceSpec` second. That ordering is what demonstrates the generality claim rather than asserting it.

Escape hatches per the `AggregateKit` precedent: `handlers=` override and `extra_ops=` merge, so an application that needs a different acceptance rule replaces one handler instead of abandoning the kit.

## 6. What stays out

- **No `AUTO` acceptance policy**, in either placement. A threshold may *rank* a review queue; it never replaces the reviewer.
- **No review UI, no notification, no assignment.** Those are application concerns and they are where the variability actually lives.
- **No proposal-specific inference surface.** Model invocation is `contracts/inference`; this composes it and adds nothing to it.
- **No `SpecPlane`.** Even under the contract answer, §3's first point holds — the state is on the document plane and the port would bind nothing.

## 7. Open questions

1. **Placement.** §3 leans kit; §4 lists what would overturn it. *Settle before any code — this is the RFC's reason to exist.*
2. **Supersession identity.** `SUPERSEDED` implies a target and a uniqueness rule (at most one accepted proposal per target). Caller-supplied key, or derived from the candidate? *Lean: caller-supplied — the framework cannot know what "the same thing" means.*
3. **Is acceptance transactional with the effect it authorises?** Accepting a mapping presumably applies it. One transaction, or accept-then-apply with idempotency? *Lean: separate, with an idempotency key — the apply is the application's and may be long-running, and welding it to the status flip makes every apply a write amplification of the review.*
4. **Evidence: one model or a sequence?** Re-evaluating a stale proposal is plausible; a single evidence field loses the history. *No lean.*

## 8. Decision log

| # | Decision | State |
| --- | --- | --- |
| 1 | The placement question is answered **before** implementation, with evidence, not after. Importance is not the test for core-hood; several backends behind one contract is | locked |
| 2 | Leaning: **kit**, not contract — no state, no plane, every operation decomposing into shipped primitives, and three shipped precedents for kit-owned `DocumentSpec`s | proposed |
| 3 | The "port enforces human acceptance" argument is **rejected as illusory**: proposals are documents and a sibling document port can flip a status regardless. Enforcement lives with whoever owns the handler in either design. Consequence — if this RFC ever flips to *contract*, it must be for a reason other than enforcement | locked |
| 4 | No `AUTO` acceptance policy, in any placement. Thresholds rank a queue; they never replace a reviewer | locked |
| 5 | Proposals are documents on the `DOCUMENT` plane — exportable, reconciled and portable for free; no new plane and no new stateful key | locked |
| 6 | Built against a **deterministic proposer first**, model-backed second. Ordering is the proof that the lifecycle does not presuppose a model | locked |
| 7 | The DST property (an un-accepted proposal changes no downstream state) is the contract's core claim and ships as a test in either placement — it does not require a port | locked |
| 8 | Overturning conditions recorded in §4: an external-system implementation, a framework-enforceable invariant a kit cannot carry, or a second in-tree consumer. Absent all three, this stays a kit | recorded |
