# RFC 0039 — Proposal lifecycle: machine-authored artifacts that stay inert until a human accepts

- **Status:** ✅ Complete — **placement settled 2026-11-26: a kit** (§7 Q1, row 9), on §3's evidence; none of §4's overturning conditions holds. §2.1 and §5.1 amend the shape from its first consumer, bi-backend's mapping proposals, and settle §7 Q2–Q4 (rows 10–17). **Shipped 2026-11-26**: `forze_kits.integrations.proposal`, with `proposal_spec`, `ProposalKit` (`request`, `run`, `decide`) and `proposal_spec_contributions`, proven on the mock and Postgres, with a model-backed proposer through `ctx.inference` after the deterministic one (row 6). Departures are in [`EXECUTION-LOG.md`](EXECUTION-LOG.md) Unit 40 (D-381 to D-387). The maintainer accepted their proposed rows on 2026-11-26 (row 15, and rows 18 and 19).
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

### 2.1 The shape, amended from its first consumer (2026-11-26)

§2 was written before a consumer existed. bi-backend has since built the lifecycle by hand for
its mapping proposals: a model reads a profile of the fields a source sends that the spec does
not read, and an analyst decides what to do with each. It differs from §2 in six places, and
the kit takes its shape, since a kit built from §2 would fit neither of its two flows:

- **A proposal is a batch of items, each with its own verdict.** An analyst accepts some of a
  proposal's mappings and rejects the rest in one decision. Deciding sets every item's verdict
  at once, an item left out is rejected, and the proposal is then `decided`. The single
  artifact of §2 is a proposal of one item: bi's second flow, a generated spec draft the
  analyst saves or not, is that case.
- **Accepting may amend.** The reviewer can accept an item with a change, and the item keeps
  both what was proposed and what was decided. A reviewer who could only say yes or no would
  reject a near-miss and redo it by hand.
- **An item may be a question.** A proposer unsure of its answer returns a question instead,
  and accepting a question requires the reviewer's answer. This is how the no-auto-path rule
  (row 4) meets a low-confidence guess: it becomes a question, never a silent default.
- **Evidence and confidence are per item.** The evidence is the facts the proposer saw for
  that item, never a verdict. Re-evaluating is a new proposal, so one evidence value per item
  is enough (§7 Q4).
- **Generation is asynchronous, and can fail.** A model call takes minutes, so a request
  creates the proposal `pending`, a worker fills its items, and it becomes `open`, or `failed`
  with the error. A second request while one is pending for the same scope and base returns
  that one; a request against a newer base gets a new proposal, since the old one is already
  stale. Because a pending proposal is reused, its run is dispatched in the transaction that
  creates it, through a durable enqueue that joins it or the transactional outbox: a requester
  that stopped between creating it and scheduling the run would otherwise leave it pending for
  good, with every later request reusing it.
- **At most one proposal is pending per scope and base.** Finding none and then creating one
  does not stop two concurrent requests from both creating one, and so both paying for a run.
  The collection therefore declares a uniqueness over scope and base among its pending rows,
  a missing base counting as one value; on Postgres it is a partial unique index. The losing
  request's create fails on it as a conflict, with nothing it dispatched committed. The
  conflict says the scope and base already have a proposal; a caller that wants it reads the
  latest proposal for them. Requesting again returns it only while it is still pending: once
  its run has finished, a request is a new proposal, as any later request is (row 13), since a
  retry and a deliberate re-evaluation look the same. A store that cannot keep a filtered
  uniqueness refuses the collection at wiring, as it would any guarantee it cannot keep.
- **A proposal is made against a base, and goes stale.** It records the version of the target
  it was proposed against. Deciding it after the target moved is refused as
  `proposal_stale`: propose again. That is the uniqueness rule `superseded` stood for (§7 Q2),
  without a status the framework would have to maintain.

What acceptance authorises splits in two (§7 Q3). The cheap effect lands with the verdicts, in
one transaction: bi saves the new spec revision and the decided proposal together, or neither.
The long effect, applying that revision to a warehouse, runs on its own, but its queue entry is
written in that same transaction, through the transactional outbox or a durable enqueue that
joins it. Queued after the commit, a crash between the two would leave an accepted change that
nothing ever applies, and nothing would find it again.

Three rules keep acceptance exact:

- **No accepted item, no effect.** A decision that rejects every item records the verdicts and
  runs no effect, so an un-accepted proposal changes nothing downstream (row 7).
- **The base is checked where the target is written.** Reading the base, comparing it and then
  writing does not stop two open proposals on one base from both being accepted, each applied
  to a target the other already moved. The effect therefore writes the target conditionally on
  the base the proposal recorded, as a compare-and-set on the target's version or a uniqueness
  on its next version, and a write that finds the base moved is refused as stale. The kit's own
  check before the effect is a fast refusal, not the guarantee.
- **One decision per proposal.** The verdicts are stored at the revision read, so of two
  concurrent decisions on one proposal the second fails before its effect runs.

The statuses are therefore `pending`, `open`, `decided` and `failed`, and an item's verdict is
`pending`, `accepted` or `rejected`.

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

### 5.1 The kit, amended (2026-11-26)

The same package, with §2.1's shape:

- `record.py` — a spec factory generic over the application's two models, what an item
  proposes and what evidence it carries, building the proposal and item models around them:
  the statuses, the verdicts, the proposed and decided values, the question, the confidence,
  the base, the scope, the error, and what generating it cost (model, tokens, latency).
- `inventory.py` — the contribution, registered `KIT`: the item models are the application's,
  so the table is the application's too (RFC 0080 row 28).
- The operations:
  - **request** creates a `pending` proposal for a scope and a base, and dispatches its run in
    the same transaction, or returns the one already pending for that scope and base, which a
    uniqueness over scope and base among pending proposals keeps to one under concurrent
    requests;
  - **run** calls the proposer for a `pending` proposal and stores its items as `open`, or its
    error as `failed`; the application calls it from its own worker or durable function;
  - **decide** takes the verdicts, refuses without an authenticated principal, refuses a
    proposal not `open` and a stale one, stores the verdicts and amendments, and runs the
    application's accept effect in the same transaction.
- The proposer is a plain callable from the request to the items, or one built from an
  `InferenceSpec`; the base check and the accept effect are callables too. Built against a
  deterministic proposer first (row 6).

## 6. What stays out

- **No `AUTO` acceptance policy**, in either placement. A threshold may *rank* a review queue; it never replaces the reviewer.
- **No review UI, no notification, no assignment.** Those are application concerns and they are where the variability actually lives.
- **No proposal-specific inference surface.** Model invocation is `contracts/inference`; this composes it and adds nothing to it.
- **No `SpecPlane`.** Even under the contract answer, §3's first point holds — the state is on the document plane and the port would bind nothing.

## 7. Open questions

1. **Placement.** §3 leans kit; §4 lists what would overturn it. *Settle before any code — this is the RFC's reason to exist.* *Settled 2026-11-26:* a kit (row 9).
2. **Supersession identity.** `SUPERSEDED` implies a target and a uniqueness rule (at most one accepted proposal per target). Caller-supplied key, or derived from the candidate? *Lean: caller-supplied — the framework cannot know what "the same thing" means.* *Settled 2026-11-26:* no `superseded` status; a proposal records a caller-supplied base and is refused as stale against a moved one (row 14).
3. **Is acceptance transactional with the effect it authorises?** Accepting a mapping presumably applies it. One transaction, or accept-then-apply with idempotency? *Lean: separate, with an idempotency key — the apply is the application's and may be long-running, and welding it to the status flip makes every apply a write amplification of the review.* *Settled 2026-11-26:* both, split by cost: the cheap effect in the decide transaction, the long one queued from it (row 15).
4. **Evidence: one model or a sequence?** Re-evaluating a stale proposal is plausible; a single evidence field loses the history. *No lean.* *Settled 2026-11-26:* one per item; re-evaluating is a new proposal (row 13).

## 8. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The placement question is answered **before** implementation, with evidence, not after. Importance is not the test for core-hood; several backends behind one contract is |
| 2 | `ASSUMED` | Leaning: **kit**, not contract — no state, no plane, every operation decomposing into shipped primitives, and three shipped precedents for kit-owned `DocumentSpec`s. Superseded by row 9 |
| 3 | `LOCKED` | The "port enforces human acceptance" argument is **rejected as illusory**: proposals are documents and a sibling document port can flip a status regardless. Enforcement lives with whoever owns the handler in either design. Consequence — if this RFC ever flips to *contract*, it must be for a reason other than enforcement |
| 4 | `LOCKED` | No `AUTO` acceptance policy, in any placement. Thresholds rank a queue; they never replace a reviewer |
| 5 | `LOCKED` | Proposals are documents on the `DOCUMENT` plane — exportable, reconciled and portable for free; no new plane and no new stateful key |
| 6 | `LOCKED` | Built against a **deterministic proposer first**, model-backed second. Ordering is the proof that the lifecycle does not presuppose a model |
| 7 | `LOCKED` | The DST property (an un-accepted proposal changes no downstream state) is the contract's core claim and ships as a test in either placement — it does not require a port |
| 8 | `ASSUMED` | Overturning conditions recorded in §4: an external-system implementation, a framework-enforceable invariant a kit cannot carry, or a second in-tree consumer. Absent all three, this stays a kit |
| 9 | `ASSUMED` | The lifecycle is a kit, `forze_kits.integrations.proposal`, not a core contract: §3's evidence holds and none of §4's conditions does. Settled by the maintainer on 2026-11-26 (§7 Q1). Supersedes row 2 |
| 10 | `ASSUMED` | A proposal is a batch of items, each with its own verdict; deciding sets them all, an item left out is rejected, and the proposal is then `decided`. A single artifact is a proposal of one item. Supersedes §2's single artifact (§2.1) |
| 11 | `ASSUMED` | The statuses are `pending`, `open`, `decided` and `failed`, and an item's verdict is `pending`, `accepted` or `rejected`. Supersedes §2's drafted, evaluated, accepted, rejected and superseded (§2.1) |
| 12 | `ASSUMED` | Accepting may amend: an accepted item keeps what was proposed and what was decided. An item may be a question, which is accepted only with the reviewer's answer (§2.1) |
| 13 | `ASSUMED` | Evidence and confidence are per item, typed by the application; re-evaluating is a new proposal (§7 Q4) |
| 14 | `ASSUMED` | A proposal records a caller-supplied base; deciding it against a moved base is refused as `proposal_stale`. There is no `superseded` status (§7 Q2) |
| 15 | `ASSUMED` | Deciding stores the verdicts and runs the application's accept effect in one transaction, and runs no effect when no item is accepted. A long-running effect is queued in that transaction, through the transactional outbox or a durable enqueue that joins it, idempotent per proposal. The effect writes the target conditionally on the proposal's base; the kit's base check is a fast refusal. Departs from §7 Q3's lean on the cheap half only (§2.1). The verdicts are stored first, at the revision read, and the effect runs after them in the same transaction, so the second of two concurrent decisions fails before its effect runs; see EXECUTION-LOG Unit 40 (D-383) |
| 16 | `ASSUMED` | Generation runs outside the request: a request creates a `pending` proposal and dispatches its run in the same transaction, or returns the one pending for its scope and base, and a run stores the proposer's items as `open` or its error as `failed`. A request against a newer base gets a new proposal. At most one proposal is pending per scope and base, kept by a uniqueness over them among pending proposals; the request that loses the race fails as a conflict, and requesting again returns the winner only while it is pending (§2.1) |
| 17 | `ASSUMED` | The spec registers as `KIT`: its item models are the application's, so its table is too (RFC 0080 row 28) |
| 18 | `ASSUMED` | The collection is a `ProposalSpec` over pydantic generic models parametrized by the application's change and evidence models; two specs over the same models are one spec. See EXECUTION-LOG Unit 40 (D-381) |
| 19 | `ASSUMED` | The proposer is a plain callable from the request to the draft, and a model-backed one is the application's function calling `ctx.inference`; there is no factory over an `InferenceSpec`. Supersedes §5's `factories.py` and §5.1's "or one built from an `InferenceSpec`". See EXECUTION-LOG Unit 40 (D-385) |

Rows 1–8 are the original decision log, regraded in this table's vocabulary on 2026-11-26
(`locked` as `LOCKED`, `proposed` and `recorded` as `ASSUMED`) with their wording unchanged.
Rows 9–17 come from the maintainer's decision on §7 Q1 and from the first consumer's flow
(§2.1). Rows 18 and 19, and row 15's last sentence, are execution's, accepted by the
maintainer on 2026-11-26.
