# RFC 0048 — Read fields no write produces: making view-backed aggregates reachable from the mock

- **Status:** 📝 Draft — the declaration is the decision; the mock's synthesis strategy is deliberately `OPEN`.
- **Scope:** A way for a `DocumentSpec` to declare that some read-model fields are **produced by the backend, not by any write** — a SQL view's joined columns — so that `forze_mock` can serve them and `forze_dst` can simulate the aggregates that carry them. Touches `forze/application/contracts/conformity/` (the leniency contract it extends), `DocumentSpec`, and `forze_mock/adapters/document.py`. **No change to real-backend strictness**: Postgres, Mongo and Firestore keep refusing a read field with no column unless it is already lenient. Not a view generator and not a relationship system — see §4.
- **Related:** `src/forze/application/contracts/conformity/lenient_read.py` (the shipped leniency mechanism this extends), `src/forze_mock/adapters/document.py:500` and `:766` (the mock already honours `resolved_lenient_read_fields`), `src/forze/application/contracts/document/specs.py` (`materialized`, `write_omit_fields`), RFC 0001 (the mock is the oracle's whole world — this is the same horizon argued from the application's side).
- **Origin:** A 159-file migration of a production ERP backend from forze 0.1.15 to 0.7.0. 14 of its 25 aggregates could not be tested against `forze_mock` at all; the suite works around it with a `hydrate_view` fixture that fakes each join by hand.

---

## 1. Summary

A read model may carry fields that no write path produces and no stored column holds: a
supplier's name reached through a foreign key, a catalogue code assembled by a view, a
group label from a parent row. Today the framework has exactly one accommodation for
this — `lenient_read_fields`, which drops the field from the read projection and
rehydrates it from the model's **default**. A field that is *required* in the read model
has no path at all, and required is what a joined display field usually is.

The consequence is not a missing convenience. `forze_mock` stores what was written and
reads it back through the read model, so an aggregate with a required derived field
cannot round-trip through the mock, which makes both `forze_mock` and `forze_dst`
unusable for it. In the one application measured, that is **over half the aggregates**.

This RFC adds a declaration: a read field may be marked as **derived**, naming where its
value comes from. Real backends treat the declaration as documentation — the view already
produces the column. The mock treats it as an instruction, and performs the join itself,
which it can do because it holds every row.

## 2. Motivation

The evidence is one migration, and it is worth stating at full strength rather than
generalising early.

An ERP backend with 25 aggregates and 146 operations moved to 0.7.0. Its read models are
Postgres views: 14 of the 25 require fields no write produces — `supplier`, `order`,
`group`, `catalog_code` among them. The migration wrote 218 tests where there had been
zero, and reached 63.6% coverage. The 14 view-backed aggregates are covered
*structurally* — their registries compose and freeze — and **not behaviourally**, because
there is no in-memory path to exercise them through. The report's own next-steps section
lists two items blocked on exactly this: Postgres-backed integration tests, and
deterministic simulation.

Two claims follow, and only the first is measured:

- **Measured.** For this application, the mock covers 11 of 25 aggregates. The other 14
  need a real database to exercise at all.
- **Argued.** A read model assembled by a view is ordinary in relational applications, so
  the mock's reach is narrower than its documentation implies for a whole class of app.
  One data point does not establish a distribution; it does establish that the
  distribution is not empty, which is enough to design against.

There is a second, sharper cost. `forze_dst` builds on the mock. An invariant written
over an aggregate the mock cannot hold is an invariant that cannot run — so the
simulation plane's coverage silently inherits this hole, and the RFC 0001 argument that
"the mock is the oracle's whole world" applies here with the mock's world cut in half.

## 3. Current state

Verified against the tree, not from memory.

**The leniency mechanism exists and is broader than one might assume.**
`read_conformity: Literal["strict", "lenient"]` and `lenient_read_fields:
frozenset[str]` live on both `DocumentSpec` and `SearchSpec`, with the shared rules in
`src/forze/application/contracts/conformity/lenient_read.py`. `resolved_lenient_read_fields`
(explicit ∪ auto-derived) is the accessor every consumer reads.

**The mock already honours it.** `src/forze_mock/adapters/document.py` passes
`lenient=self.spec.resolved_lenient_read_fields` at two call sites (`:500`, `:766`). This
is the fact that reframes the problem: the mock is not ignorant of leniency, and the fix
is not "teach the mock about leniency".

**The blocking rule is explicit and deliberate.** `validate_lenient_read_fields` refuses
a required field:

```python
if field.is_required():
    raise exc.configuration(
        f"Lenient read field {name!r} has no default (spec {spec_name!r}); a "
        "field absent from storage must be constructible from a default.",
    )
```

and `derive_lenient_read_fields` only derives fields that are non-required *and* carry a
static default (a `default_factory` is excluded, because it would yield a fresh value per
row). So `read_conformity="lenient"` does nothing whatsoever for `supplier: str`.

**Why the rule is right as far as it goes.** Leniency's contract is "absent from storage,
reconstructed from the model". A required field has nothing to reconstruct *from*. The
rule is not an oversight to delete — it is the correct rule for a mechanism whose only
source of truth is the model's own default. What is missing is a *second* source: the
other rows.

**What the neighbouring knobs do, and why none of them fits.**

| Knob | What it means | Why not this |
|---|---|---|
| `lenient_read_fields` | absent from storage, filled from the model default | needs a default; a required join field has none |
| `write_omit_fields` | a domain field deliberately not stored, silently dropped on write | the field is not a domain field here, and dropping is not the problem |
| `materialized` | a computed field *persisted* so it can be filtered and sorted | inverted: this is a stored field derived from the same row, not a field derived from another row |

## 4. Goals / Non-goals

**Goals**

- A view-backed aggregate round-trips through `forze_mock` without the application
  writing a fixture that fakes the join.
- The declaration is checkable at wiring time: a derived field that names an
  unreachable source fails the boot, not the first read.
- Real backends are unchanged. A Postgres view already produces the column; the
  declaration must not make the strict validator laxer.
- `forze_dst` reaches the aggregates the mock newly holds, with no DST-specific work.

**Non-goals**

- **Not a view generator.** The framework does not emit `CREATE VIEW`. Where the joined
  column comes from on a real backend stays the application's schema problem — this RFC
  only describes what the field *is* so the mock can stand in.
- **Not a relationship / ORM system.** A derived field names one source field reached by
  one key. Anything needing a general object graph is out; §8 records the boundary.
- **Not a change to write paths.** A derived field is never written, never patched, and
  never part of a write model.
- **Not leniency's replacement.** `lenient_read_fields` keeps its meaning and its
  required-field refusal. This is a sibling declaration, and §5.1 states how they compose.

## 5. Design

### 5.1 The declaration

A new spec-level field, alongside `lenient_read_fields` rather than inside it, because
the two answer different questions — leniency says *may be absent*, derivation says
*comes from there*:

```python
@attrs.define(slots=True, kw_only=True, frozen=True)
class DerivedReadField:
    """A read field produced by the backend from another relation."""

    source: NamedResourceSpec | str
    """The spec (or spec name) holding the value."""

    via: str
    """The field on *this* aggregate carrying the source's key."""

    field: str
    """The field on the source read model whose value lands here."""

    optional: bool = False
    """When the key is nullable: a missing source row yields ``None`` rather than a refusal."""
```

and on `DocumentSpec`:

```python
derived_read_fields: Mapping[str, DerivedReadField] = {}
```

Guardrails, at definition time, reusing the shape of `validate_lenient_read_fields`:

- The key must be a non-computed field on the read model, and must **not** be an
  identity/audit field (`IDENTITY_READ_FIELDS`) — the same refusal leniency already makes.
- The key must **not** also be in `lenient_read_fields`, `materialized`, or
  `write_omit_fields`. Two mechanisms claiming one field is a configuration error, not a
  precedence puzzle.
- `via` must be a real, non-computed field on this aggregate's read model.
- A derived field **may** be required. That is the whole point, and it is what
  distinguishes this from leniency.
- `optional=False` with a nullable `via` field is refused: the pairing is a read that
  will fail on the first unset key, and refusing it at the boot is the cheaper failure.

The read field is excluded from the filter/sort/aggregate sets the same way lenient
fields are, through the existing shared `_read_query_fields()` subtraction — a derived
field is not operative, and a query over it would be a query the mock could answer and
Postgres could not. §10 carries the one case this forecloses.

### 5.2 What each backend does with it

**Postgres, Mongo, Firestore: nothing at runtime.** The view or the pipeline already
produces the column, so the field is read from storage as it is today. Two changes, both
at the boundary:

- The Postgres startup schema validator must not demand a *write* column for a derived
  field (it is read-only), and must still demand the column exist on the read relation —
  the declaration is a claim about where the value comes from, not permission for it to
  be missing.
- Nothing is threaded into the gateways. This is what keeps the blast radius small.

**The mock performs the join.** `MockState` holds every row of every spec, so the join is
genuinely available to it — this is the asymmetry the design exploits. On read, for each
derived field: take `via` off the stored row, look up `source` by primary key, and take
`field` off it. A missing source row is `None` when `optional`, and a refusal otherwise
(`exc.internal`, naming the spec, the field and the key — a dangling key in the mock is a
seeded-data bug, and the message should say so).

### 5.3 The cost this pays, stated plainly

A mock that performs joins is a mock that can diverge from the real join. The RFC accepts
that, with two bounds:

- The join is by primary key only. No predicates, no ordering, no aggregation — those are
  the cases where a hand-rolled join and a SQL view genuinely differ.
- Divergence is exactly what the mock↔real differential (RFC 0001) exists to catch, and a
  derived field is a read-value comparison, which is the differential's easiest case.

### Alternatives considered

**A — require a default, change nothing.** Tell application authors to write `supplier:
str = ""`. Zero framework code. It was rejected on honesty rather than effort: an empty
supplier is indistinguishable from an unjoined one, every DST invariant over that field
becomes vacuously satisfiable, and the framework would be asking applications to weaken
their own types to fit a testing limitation. The trade-off is real, though — this is the
option with no maintenance cost, and an application that only wants the aggregate to
*load* should still be told it exists.

**B — the mock relaxes read validation to the persisted subset.** Return a partial model,
or a different type, for rows the mock cannot complete. Rejected because it breaks the
one contract the read model has: a handler receiving `R` may read `R`'s fields. A test
passing against a laxer shape than production is the failure mode this codebase has
recorded repeatedly, and it would move the mock from "narrower than reality" to
"different from reality", which is worse.

**C — a per-spec synthesizer callable.** `derived=lambda row, state: …`. More expressive
than a declared join and strictly worse as a contract: it is unavailable to the startup
check, unavailable to the differential, and it puts test-shaped code in a spec that
production also reads. Kept in §8 as the escape hatch, unbuilt.

## 6. Tests

- **Spec guardrails** in `tests/unit/test_forze/application/contracts/test_document_spec.py`,
  beside the existing leniency guardrail cases: each refusal in §5.1, and the accepted
  required-field case that leniency refuses — the two sitting next to each other is the
  documentation of the distinction.
- **Mock round-trip**: create a parent and a source row, read the parent back, assert the
  derived value; then the `optional` path, and the dangling-key refusal.
- **The claim that actually matters**: an aggregate shaped like the ERP's — required
  derived field, no default — is created, read, filtered and paged through the mock. This
  is the test whose absence is the motivation, so it is written first and must fail
  against `main`.
- **Differential**: the derived field joins the existing mock↔real document conformance
  battery as a read-value case (RFC 0001's family), which is what bounds §5.3.
- **Explicitly not tested**: that a Postgres view produces the same value as the mock's
  join for an arbitrary view. That is unbounded, and the differential covers the shapes
  the battery declares.

## 7. Docs

- `pages/docs/data-events/reading-data.md` gains the distinction between the three
  read-shape knobs, because a reader who has to guess between `lenient_read_fields`,
  `materialized` and `derived_read_fields` will guess wrong.
- `pages/docs/testing/overview.md` must **stop implying the mock covers every
  aggregate** and say what makes one unreachable. This is the honesty half of the change
  and it should land even if the mechanism is deferred.
- The published skill's `testing-with-mock` reference gains the same, in one paragraph.

## 8. Out of scope

- **A synthesizer callable** (alternative C) — named as the escape hatch, not built. What
  would change it: a real case where a declared key join cannot express the derivation and
  the aggregate matters enough to test.
- **Derived fields on `SearchSpec`** — the same argument probably applies to a search
  return shape, and no evidence has been offered for it. Deliberately not generalised
  ahead of a second data point.
- **Multi-hop derivation** (`order.supplier.name`) — one hop only. A second hop is a
  relationship system, and §4 rules that out.
- **Making the query planner answer filters over derived fields.** §5.1 excludes them
  from the operative sets; §10 asks whether that is the right call.

## 9. Risks

- **The mock becomes a small ORM.** Every join feature added here makes the mock's
  divergence surface larger and its failures less like the real backend's. Mitigation:
  the primary-key-only bound in §5.3 is a `LOCKED` decision, so widening it costs an
  amendment rather than a patch.
- **The declaration reads as a promise the framework does not keep.** An author may see
  `derived_read_fields` and expect the framework to *produce* the view. Mitigation: the
  name, the docstring and the docs all say "declares where the backend gets it"; §7 makes
  this a doc requirement rather than a hope.
- **It fixes the mock and leaves DST's harder half.** Holding the row is necessary and
  possibly not sufficient for a useful simulation of a view-backed aggregate. Accepted:
  P2 is scoped to find out, and finding out is worth more than assuming either way.

## 10. Unresolved questions

- **Should a derived field be filterable at all?** §5.1 excludes it. A view's joined
  column genuinely *is* filterable in Postgres, so the exclusion makes the mock's answer
  narrower than production's rather than wider — the safe direction, and still a real
  capability loss for an application that filters by supplier name. Settled by asking the
  origin application whether any of its 14 aggregates filters on a derived field.
- **Does the ERP's `catalog_code` fit the one-hop key join at all?** "Assembled by a view"
  may mean concatenation, not a join. If several of the 14 are computed rather than
  joined, §5.1's shape is wrong for them and alternative C moves from escape hatch to
  design. **This is the question that decides whether the RFC is ready to execute**, and
  it is answerable by reading one schema.
- Whether the startup validator's read-relation check can distinguish a view's column
  from a table's without new configuration.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | **Superseded by row 11.** Derivation is a **separate declaration** from `lenient_read_fields`, and the required-field refusal in `validate_lenient_read_fields` stays exactly as it is. Leniency's source of truth is the model default; derivation's is another row. Merging them would give one knob two incompatible meanings, and the error message that currently explains the refusal would have to stop being true. |
| 2 | `LOCKED` | A derived field **may be required**. This is the entire capability — every field in the motivating application is required — and a design that still demanded a default would restate the problem as its solution. |
| 3 | `LOCKED` | **Real backends are unchanged at runtime.** No gateway threading, no laxer read projection. The declaration is documentation to Postgres, Mongo and Firestore, and an instruction only to the mock. Consequence: an application whose view does *not* produce the column still fails against the real backend, which is correct and must stay correct. |
| 4 | `LOCKED` | The mock's join is **primary-key only** — no predicates, ordering or aggregation. Locks the mock out of expressing a filtered or ordered derivation; changing it later means re-arguing §5.3's divergence bound, not just adding a parameter. |
| 5 | `ASSUMED` | A dangling key in the mock is a **refusal**, not a silent `None`. Believed right because it is a seeded-data bug and silence would make it a mystery on the next assertion; execution may depart if a real fixture ordering makes the refusal impractical, and should log why. |
| 6 | `ASSUMED` | Derived fields are excluded from the filter/sort/aggregate sets, reusing the existing `_read_query_fields()` subtraction. §10 may overturn this row on evidence from the origin application. |
| 7 | `OPEN` | **How the mock reads the source row** — through the same `MockState` document store the source spec's own adapter uses, or through a lower-level index. Execution decides. What settles it: whether a tenanted source spec's row is reachable without re-entering the adapter's tenancy resolution, since a derived read must not cross a tenant boundary the source adapter would have refused. |
| 8 | `OPEN` | Whether `optional=False` plus a nullable `via` field is refused at definition time (§5.1's proposal) or only warned. Execution decides on how many legitimate shapes the refusal breaks. |
| 9 | `LOCKED` | **A derived field carries one scalar value, and that shape serves the scalar case only.** §10's first unresolved question is answered against the origin schema: `catalog_code` is exactly `c.code` off a joined row, so the design is not wrong — but 27 of that application's derived fields are nested reference *objects* projecting a field subset of the source row, 12 of them required, spread across all 14 view-backed aggregates. P1 therefore unblocks the scalar case, not the motivation's "14 of 25". Consequence: the §2 reach claim is not what P1 delivers, and §12 gains a phase before it does. Added by execution 2026-09-11 — see logs/T-0048.md (§10(b), attempt 1). |
| 10 | `LOCKED` | **A source relation may itself be derived**, so a faithful join can be two hops deep in derived data (`v_supply_order_items.detail` joins `v_details`). The primary-key bound of row 4 is unchanged and the mock reads a source row straight from its namespace without hydrating that row's own derived fields. Recorded as a known limit: resolving them is the recursion §4's non-goals rule out. Added by execution 2026-09-11 — see logs/T-0048.md (D-4, attempt 1). |
| 11 | `LOCKED` | **Marking and resolving are separate, and marking is the floor.** Supersedes row 1's shape (not its separation from leniency, which stands). A field mapped to `None` is declared produced-by-the-relation and nothing more: real backends are unchanged, every not-stored-here guard applies, and in the mock the value comes from the stored row. Marking is shape-independent, so a nested reference, a `COALESCE` aggregate and a `CASE` expression are one declaration; resolving stays available for the one-field-one-row-one-key case. Consequence: the framework never evaluates a view expression, and the pressure to grow the declaration toward SQL is answered with "seed it" instead. Added by execution 2026-09-11 — see logs/T-0048.md (D-1, attempt 2). |
| 12 | `ASSUMED` | **A marked field's value is seeded, through a stated carve-out in the seed applier.** `SpecSeed.derived` writes onto the stored row after the command port has created it, because a derived field is refused on a create command and so has no port path. The applier's "never into `MockState` directly" rationale is about fields the write path produces, which a derived field definitionally is not. Consequence: the seeder mints the id for such a spec (deterministically, under the bound entropy source) because `create(return_new=True)` would read the row back before the values exist. Added by execution 2026-09-11 — see logs/T-0048.md (unlisted, attempt 1). |
| 13 | `ASSUMED` | **A required marked field left unsupplied is refused by name** (`mock.document.derived_unsupplied`), not left to surface as a pydantic missing-key error. The declaration is what put the field beyond the write path, so the declaration is what the message should name. Optional fields are untouched — absence is what `None` is for. Added by execution 2026-09-11 — see logs/T-0048.md (A-6). |
| 14 | `ASSUMED` | **A marked field's value may come from a registered stand-in for the relation, consulted per row.** Seeding cannot reach a row created after it ran, and under simulation the workload creates every row there is, so an invariant over a view-backed aggregate could not run at all. `MockDepsModule(derived_values=MockDerivedRegistry())` registers a source per spec; a stored or staged value outranks it, a spec that registers nothing keeps row 13's refusal, and a source's keys are scoped to the declared derived names so it cannot fill an undeclared field. The source is fixture logic, not a derivation, and keeping it a function of the row is what keeps a simulation replayable. Added by execution 2026-09-11 — see logs/T-0048-P2.md (unlisted, attempt 1). |
| 15 | `ASSUMED` | **The seeder looks for the mock adapter behind the runtime's wrappers.** Row 12's carve-out writes onto the stored row through the concrete adapter, and a handler never holds it: tracing, resilience and a simulation's fault interceptors each wrap it, so `SpecSeed.derived` was unreachable from a `Simulation.setup` hook. The narrowing unwraps `PortProxy.inner` first and still refuses a genuinely non-mock port. Added by execution 2026-09-11 — see logs/T-0048-P2.md (unlisted, attempt 1). |

## 12. Phasing

- **P1 — the declaration and the mock (one PR).** `DerivedReadField`, the `DocumentSpec`
  field, the guardrails, the mock's read-side join, the Postgres validator's read-only
  allowance, the tests in §6 and the docs honesty in §7. Independently useful: it is what
  unblocks behavioural tests for the 14 aggregates.
- **P2 — DST reach (shipped 2026-09-11).** Confirm an invariant over a view-backed aggregate runs, and fix what
  it turns out to need. Gated on P1, and scoped to *finding out* rather than to a promised
  outcome (§9). What it turned out to need: rows 14 and 15.
- **P1a — the marker and its seed (shipped 2026-09-11).** The floor of row 11: `None`
  marks a field derived, `SpecSeed.derived` supplies it, and the refusal in row 13 makes
  an unsupplied one discoverable. This is what actually reaches the aggregates §2 counts,
  and it reaches the aggregate and `CASE` tail too, which no resolver would.
- **P2a — projected field sets.** Superseded in priority by P1a and kept as a
  convenience: `field` as a field *set* would let the six `*_json(...)` shapes be joined
  rather than seeded. Demand-gated now rather than required, since the marker already
  unblocks them.
- **P3 — search parity.** `SearchSpec.derived_read_fields`, **demand-gated** on a second
  application asking for it (§8).
