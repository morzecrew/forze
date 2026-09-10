# RFC 0050 — Child collection reconciliation

- **Status:** 📝 Draft — **not execution-ready.** The mechanism is clear; §10 carries three questions that decide its shape, and one of them depends on a stage the framework does not have.
- **Scope:** A `forze_kits` component that reconciles a parent aggregate's child collection against an incoming list — diff by key, delete the dropped, create the added, update the changed — over the bulk document ports that already exist. Touches `forze_kits` only; **no contract change** and no new port. Deliberately not a relationship system, not a cascade, and not an ordering framework — §4.
- **Related:** `src/forze/application/contracts/document/ports.py:358` (`create_many`), `:608` (`update_many`, taking `Sequence[KeyedUpdate[U]]`), `:816` (`kill_many`), `src/forze/application/contracts/document/value_objects.py:40` (`KeyedCreate`), `src/forze_kits/aggregates/soft_deletion/` (`build_soft_deletion_registry`, the delete path for specs carrying `is_deleted`); `src/forze_kits/aggregates/kit.py` (the composition surface this would sit beside); RFC 0013 (`AggregateKit`, the precedent for one declaration standing in for a hand-written slice).
- **Origin:** A production ERP backend where nine services implement the same eighty lines: supply, outsource, manufacturing (order and task), acceptance, reserving, shipment, replacement, and specification eBOM. Roughly 700 lines, with the position and ordering rules re-derived per aggregate.

---

## 1. Summary

Nine services in one application implement one procedure: read the current children by
foreign key, diff them against an incoming list, delete what was dropped, create what was
added, update what changed on a declared set of fields, then touch the parent. The
framework already ships every primitive it needs — `create_many`, `update_many` with
`KeyedUpdate`, `kill_many` — and ships no assembly of them.

This RFC proposes that assembly. It is deliberately filed as **not execution-ready**: the
procedure is easy to describe and the interesting decisions are the ones the origin
application made nine times without writing down — how an incoming child without an id is
matched, what owns ordering, and which stage of the pipeline the reconciliation belongs
to. The last of those is currently blocked on a stage the framework does not have.

## 2. Motivation

Seven hundred lines of one procedure, written nine times, is the measured cost. The
argument for centralising it is not the line count.

It is that the nine copies **disagree**. The origin report notes the position and
ordering rules are "currently re-derived per aggregate" — so nine implementations of one
invariant, each of which can be subtly wrong on its own, and none of which is the
reference. A framework component would make that one rule with one test.

The counter-argument deserves its place here: eighty lines of explicit diff-and-write is
readable, debuggable, and belongs to the application that owns the invariant. A kit that
hides it behind four constructor arguments trades that for a configuration surface, and
`AggregateKit` — the closest precedent — was **deliberately not used** by this very
application, because its `search=` arm assumed an external index this app does not have.
That is a real warning about kits that assume the shape of the app around them, and §9
takes it seriously.

## 3. Current state

**The primitives exist.** `KeyedCreate` and `KeyedUpdate` are the bulk write shapes;
`update_many(updates: Sequence[KeyedUpdate[U]], *, return_new=…, return_diff=…)` and
`create_many` are on the document command port, and the soft-delete registry handles the
delete side for the eleven aggregates carrying `is_deleted`.

**No assembly exists.** A search for child-collection handling across `src/forze_kits/`
returns nothing: there is no helper, no step, and no kit arm for a parent/child write.
The application-side pattern is unrepresented in the framework.

**The stage question is live and unresolved elsewhere.** The origin application had to
adapt every result-replacing effect onto `wrap` middleware, carrying list order by
descending step priorities, because `OnSuccess` returns `None` and cannot replace the
result. Ten of its thirty-two effects genuinely replace the result — and the
parent/child reconciliations are among them, since they reconcile children and then
re-read the parent. A separate proposal (an `AfterStep` with an `(args, result) -> R`
contract) is not in this batch, and §10 records that this RFC's shape depends on it.

## 4. Goals / Non-goals

**Goals**

- One declaration reconciles a child collection, over the existing bulk ports.
- The position/ordering rule is stated once, in one place, with one test.
- The reconciliation rides the ambient transaction, so a partial diff cannot commit.
- An application can adopt it for one aggregate without adopting `AggregateKit`.

**Non-goals**

- **Not a relationship system.** One parent, one child spec, one foreign key. No nesting,
  no many-to-many, no graph.
- **Not a cascade.** Deleting a parent is not this component's business.
- **Not an ordering framework.** It carries *a* position rule (§10 decides which), not a
  configurable ordering strategy.
- **Not part of `AggregateKit`.** It must be usable standalone, for the reason §2 gives:
  the origin application could not adopt the kit at all.

## 5. Design

### 5.1 The declaration

```python
@attrs.define(slots=True, kw_only=True, frozen=True)
class ChildCollectionSync[Parent, Child]:
    child_spec: DocumentSpec
    """The child aggregate."""

    parent_fk: str
    """The field on the child holding the parent's id."""

    key_field: str
    """The field identifying a child within the collection — its business key."""

    compare_fields: frozenset[str]
    """The fields whose change makes an existing child an update rather than a no-op."""

    position_field: str | None = None
    """When set, the child's index in the incoming list is written here."""
```

### 5.2 The procedure

Read the current children by `parent_fk`. Partition the incoming list against them by
`key_field`:

- **present in both** — compare `compare_fields`; if any differs, a `KeyedUpdate`.
  Unchanged children are not written, which is what makes the diff worth doing rather
  than deleting and recreating.
- **incoming only** — a `KeyedCreate`.
- **current only** — a delete, through the soft-delete path where the child spec carries
  it and a hard `kill_many` where it does not.

Then, in one batch each: `kill_many`, `create_many`, `update_many`. Order matters if
`key_field` has a uniqueness constraint — deletes must precede creates, or a re-added key
collides with the row being removed. That is a `LOCKED` decision below because getting it
backwards produces a failure that only appears under a specific edit.

`position_field`, when set, is written for every child whose index changed — which means
a reorder is an update of every child after the moved one, and the component should say so
rather than pretend a reorder is cheap.

### 5.3 What it does not do

It does not touch the parent, and it does not re-read it. Both belong to the caller,
because both depend on the stage question in §10: a component that re-reads the parent and
returns it is only usable from a stage that can replace the result, and no such stage
exists today.

### Alternatives considered

**A — an arm on `AggregateKit`.** Cheaper to build and unusable by the application that
needs it, for the reason recorded in §2. Rejected on that evidence directly.

**B — delete-all-and-recreate.** Far simpler: no diff, no `compare_fields`, no update
batch. Rejected because it destroys child ids and anything referencing them, and rewrites
`rev` and audit fields for untouched rows — which turns every parent save into a full
child-history rewrite. Worth stating because it is what a first implementation reaches for.

**C — leave it to applications, and document the pattern instead.** A recipe page showing
the eighty lines. Genuinely attractive: no configuration surface, no kit assuming the app's
shape, and the invariant stays where it is owned. It loses the one thing §2 says matters —
nine copies cannot disagree with each other if there is only one — and a recipe cannot be
tested against the ports the way a component can.

## 6. Tests

- Each partition in isolation: added, dropped, changed, unchanged-and-not-written. The
  last is the one that catches a "diff" that is really a rewrite.
- The delete-before-create ordering, against a child spec with a unique `key_field` and an
  edit that removes and re-adds the same key. This is the decision-5 test and it must fail
  if the order is swapped.
- Soft-delete and hard-delete child specs, since the delete path differs.
- Transaction: a failure in the update batch leaves no create committed.
- `position_field` on a reorder, asserting the *set* of writes — that is where "a reorder
  is cheap" would be caught as false.
- Against the mock, and against Postgres for the uniqueness-ordering case, which the mock
  cannot express.

## 7. Docs

- A recipe page — this is a procedure, and the recipe archetype is where a procedure
  belongs. It should show the hand-written version first and the component second, because
  an author who cannot see what it replaced cannot judge whether to adopt it.
- The published skill gains a reference only once the shape is settled; documenting an
  `OPEN`-heavy component invites the wrong pattern into applications.

## 8. Out of scope

- **Nested collections** (a child with its own children). One level. What would change it:
  evidence that an application needs two, which the origin's nine cases do not show.
- **Reordering as a distinct operation.** §5.2 makes a reorder fall out of the position
  write; a cheaper dedicated path is a later optimisation with no measurement behind it.
- **Diffing by structural equality** instead of `compare_fields`. Named because it looks
  simpler and silently makes every computed or audit field a change trigger.

## 9. Risks

- **A kit that assumes the shape of the app around it.** The precedent is direct:
  `AggregateKit` failed closed for the origin application over an assumption about search.
  Mitigation: standalone use is a goal (§4), and the component takes specs and field names
  rather than a composed registry.
- **The configuration surface replaces the readable code with a puzzle.** Four
  constructor arguments encoding a procedure is worse than eighty explicit lines *if* the
  arguments do not cover the case. Mitigation: §7's docs requirement to show both, and a
  deliberate refusal to add options — every new parameter is evidence the component is the
  wrong shape.
- **It lands before the stage it needs.** Built now, every caller wires it through `wrap`
  with hand-managed priorities, which is the trick §3 says applications should not have to
  rediscover. §12 gates on this rather than working around it.

## 10. Unresolved questions

These are why the status line says not execution-ready.

- **How is an incoming child without an id matched?** §5.1 assumes a business
  `key_field`. If a child collection has no business key — a list of free-text lines, say —
  then "changed" and "removed and added" are indistinguishable, and the component either
  demands a key it cannot always have or falls back to positional identity, which is a
  different and worse semantic. **Answerable by looking at the nine origin cases**: if any
  reconciles a keyless collection, §5.1's shape is wrong.
- **What owns the position rule?** The origin report says it is re-derived per aggregate,
  which means the nine copies may already disagree — so there may be no single rule to
  centralise. Reading two of them settles it.
- **Which stage does this run in?** Answering "the one that can replace the result" means
  waiting for that stage to exist. Until then, the honest options are: build it
  parent-agnostic (§5.3) and let callers re-read, or wait. This RFC proposes the first
  and gates the second in §12.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | It is a **`forze_kits` component, not a port and not a contract change.** Everything it needs is on the existing document command port; adding a port for an assembly of three calls would put an application procedure in the contract layer. |
| 2 | `LOCKED` | **Usable standalone**, without `AggregateKit`. The origin application could not adopt that kit at all, so a component reachable only through it would not solve the case that motivates it. |
| 3 | `LOCKED` | **One level, one foreign key.** No nesting, no many-to-many. Locks the component out of graph-shaped data; changing it later is a different design, not a parameter. |
| 4 | `LOCKED` | **Unchanged children are not written.** The whole value over delete-and-recreate is that ids, revisions and audit fields survive a parent save. |
| 5 | `LOCKED` | **Deletes are issued before creates.** A removed-and-re-added business key collides otherwise, and only under a specific edit — the failure is rare enough to survive review, which is why the order is locked rather than assumed. |
| 6 | `ASSUMED` | The component **does not touch or re-read the parent** (§5.3). Believed right because it keeps the component usable from any stage; execution may depart if every real caller ends up writing the same parent touch, which would be evidence the boundary is in the wrong place. |
| 7 | `OPEN` | **Identity of an incoming child** — a required business `key_field`, or a fallback for keyless collections. §10 names what settles it. An executor reaching this must decide and log; it is the decision most likely to make the component the wrong shape. |
| 8 | `OPEN` | **The position rule** — index in the incoming list, a sparse ordering, or no ordering support in P1. Settled by reading two of the origin implementations. |
| 9 | `OPEN` | Whether the delete side goes through the soft-delete registry when the child spec carries `is_deleted`, or whether the caller declares it. Execution decides; the specs know, so inferring it is probably right and probably wants a refusal when the spec's soft-delete is not wired. |

## 12. Phasing

- **P0 — settle §10, before any code.** Read the nine origin implementations for the key
  and position questions. This is a reading task with a written answer, and it is the
  whole gate: three `OPEN` rows on a component this small means the design is not ready,
  not that execution is free.
- **P1 — the component, parent-agnostic.** §5's procedure over the existing bulk ports,
  the §6 tests, the recipe. Gated on P0.
- **P2 — the stage.** Revisit §5.3 once a result-replacing after-stage exists. Until then
  callers re-read the parent themselves, and the recipe says so plainly rather than
  showing the `wrap`-priority trick as though it were the intended shape.
