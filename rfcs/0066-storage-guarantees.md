# RFC 0066 — Storage guarantees: the declared half of the capability convention

- **Status:** 📝 Draft — extracted from [0052](0052-versioned-facts-correction-lineage.md), [0053](0053-temporal-validity.md) and [0063](0063-per-owner-write-serialization.md), which each grew a private version of it. **Second in the batch's execution order**, after [0067](0067-period-and-overlap.md) and before every kit that declares one.
- **Scope:** A spec-level vocabulary for properties a *store* must enforce — uniqueness over a filtered set, non-overlap of periods, per-key write serialization — plus the reconciliation that checks them against each adapter's declared capability at wiring and validates the physical mechanism at startup. Adds one contract package (`contracts/guarantees/`), one field on the specs that take guarantees, one `*Capabilities` member per participating adapter, and the validators. **Plane-neutral by construction and backend-blind by rule:** no guarantee names an index, a constraint, an extension or a lock.
- **Related:** [`src/forze/application/contracts/querying/capabilities.py:9-22`](../src/forze/application/contracts/querying/capabilities.py) (the convention this is the other half of: "each backend publishes what its renderer can compile … raising a clean `precondition` naming the feature and backend. The in-memory mock is the canonical superset"), `:101` (`QueryCapabilities`), `:145` (`FULL_QUERY_CAPABILITIES`), and the eight sibling declarations — [`search/capabilities.py:60`](../src/forze/application/contracts/search/capabilities.py), [`graph/capabilities.py:25`](../src/forze/application/contracts/graph/capabilities.py), [`transaction/ports.py:73`](../src/forze/application/contracts/transaction/ports.py), [`secrets/capabilities.py:37`](../src/forze/application/contracts/secrets/capabilities.py), [`dlock/value_objects.py:12`](../src/forze/application/contracts/dlock/value_objects.py), [`stream/capabilities.py:9`](../src/forze/application/contracts/stream/capabilities.py), [`sandbox/capabilities.py:77`](../src/forze/application/contracts/sandbox/capabilities.py), [`durable/function/run_admin.py:113`](../src/forze/application/contracts/durable/function/run_admin.py); [`src/forze/application/contracts/inventory/value_objects.py:13-40`](../src/forze/application/contracts/inventory/value_objects.py) (`SpecPlane` — the fourteen planes a guarantee could reach); [`src/forze_postgres/kernel/catalog/validation/validate_schema.py:266`](../src/forze_postgres/kernel/catalog/validation/validate_schema.py) (`validate_postgres_document_schemas`, the startup refusal this joins); [`src/forze_mongo/kernel/validate_indexes.py:46`](../src/forze_mongo/kernel/validate_indexes.py) (`validate_mongo_document_indexes`, which lists indexes and warns); [`src/forze/application/execution/operations/wiring.py:79-123`](../src/forze/application/execution/operations/wiring.py) (`WiringReport`, `check_wiring`); [RFC 0067](0067-period-and-overlap.md) (what `NonOverlapping` means).
- **Origin:** Three kits distilled from a working-time ledger each needed the store to enforce something its declaration promised — one current row per fact, no overlapping validity, one writer per owner — and each proposed its own way to say so. The first draft of the vocabulary also spelled Postgres (`where="is_current"`, `EXCLUDE USING gist`, `btree_gist`) on a contract that must not know those words, which is what prompted the extraction.

---

## 1. Summary

A spec declares what its store must guarantee. Each adapter declares which guarantees it can
enforce, the way it already declares which filter operators it can compile. Wiring reconciles the
two and refuses a declaration no adapter can keep; startup validates that the physical mechanism
is actually there; the mock enforces every guarantee in memory, so a simulation sees the same
rules a deployment does. Nothing creates DDL.

## 2. Motivation

A kit that promises a property its store does not enforce is worse than a kit that promises
nothing, because the declaration reads as a guarantee. All three source kits hit this:

- "current" as a stored flag is only single-valued if the store refuses a second current row;
- effective-dated validity is only non-overlapping if the store refuses an overlapping insert;
- per-owner serialization is only serialization if every writer takes the same lock.

Each of those is a property of the *data*, expressible without naming a mechanism — and each
needs three things that no single feature should own: an adapter mapping, a startup check, and a
refusal when the deployment forgot the migration.

The framework already solved the mirror-image problem. Nine planes declare capabilities — what an
adapter *can* do — with a validator that refuses before the operation, a named error code, the
mock as canonical superset, and a parity suite that reads the flags to decide which cases it must
reproduce. Guarantees are the other direction: what a spec *needs*. Building them as a second,
private mechanism per kit is how three kits end up with three vocabularies; building them as the
declared half of an existing convention is one primitive with a precedent.

## 3. Current state

**The capability convention is shipped, consistent, and one-directional.**
`QueryCapabilities` and its eight siblings are declared per adapter, checked by a validator that
raises `precondition` naming the feature and the backend (`query_feature_unsupported`), with
`FULL_QUERY_CAPABILITIES` as the mock's superset. What no plane has is the *inverse* — a spec
saying "I require this of whatever store serves me" — so a requirement has nowhere to live and
nothing to reconcile against.

**Startup validation exists per backend, and validates only columns.**
`validate_postgres_document_schemas` raises `internal` when a model field has no column, and
checks the history relation and materialized fields. `validate_mongo_document_indexes` lists
indexes and *warns* about secondary unique ones. Neither takes a declared requirement, so
"the deployment forgot the partial unique index" is discovered as a production race.

**`BackendRequirements` is not this.**
[`kit.py:103`](../src/forze_kits/aggregates/kit.py) is a *wiring* checklist — which routes, which
tx route, whether a keyring is needed — and its docstring is explicit that it describes what to
wire "without fabricating the backend-specific config objects … whose values only the author
knows". A storage guarantee is a property of the data, not a route to bind.

**Nothing creates schema, anywhere.** No adapter issues DDL for a spec today, and this RFC does
not change that — §5.4 states it as a rule rather than an omission.

**Fourteen planes exist**, of which several could carry a guarantee: `DOCUMENT`, `GRAPH`,
`COUNTER`, `STORAGE`, and arguably `SEARCH`. The vocabulary is therefore defined on the guarantee,
not on `DocumentSpec` (§5.2).

## 4. Goals / Non-goals

**Goals**

- One vocabulary for "the store must enforce this", usable by any spec plane that has a store.
- Not one word of backend syntax in it — no index, constraint, extension, lock or range type.
- Reconciliation at wiring against the adapter's declared capability, with a named refusal.
- Validation at startup that the mechanism exists, naming what would satisfy it.
- The mock enforces every guarantee, so DST tests the rule rather than the adapter's absence.

**Non-goals**

- **Not migrations.** Nothing is created, altered or dropped. §5.4.
- **Not application invariants.** A law over a read-set is `SystemInvariant`; a guarantee is what a
  store refuses at write time. The two are complementary and are not merged — §5.6.
- **Not a query feature.** A guarantee is not pushed down into a filter; a store that can
  *enforce* uniqueness need not be able to *query* it any differently.
- **Not a capability audit.** Whether the framework's nine capability declarations should be
  unified is a separate question this RFC deliberately does not open; it adds a tenth in their
  shape.
- **Not per-instance policy.** A guarantee belongs to a spec, not to a route, a tenant or a call.

## 5. Design

### 5.1 The vocabulary

```python
UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
UniqueTogether(fields=("supersedes_id",), skip_null=True)
NonOverlapping(key=("employee_id",), period=("valid_from", "valid_to"), bounds="[]")
SerializedBy(key=("employee_id",))
```

Three members, each a property with no mechanism in it:

- **`UniqueTogether`** — at most one row per field tuple among the rows matching `where`. `where` is
  an ordinary `QueryFilterExpression`, which every adapter already parses, so a *filtered*
  uniqueness needs no new syntax and no per-backend spelling.
- **`NonOverlapping`** — no two rows sharing `key` have overlapping periods, where "overlapping" is
  [RFC 0067](0067-period-and-overlap.md)'s definition and `bounds` is its convention. The
  guarantee names two fields and a convention; the adapter chooses a range type if it has one.
- **`SerializedBy`** — writes to this spec for one `key` value do not interleave. A property, not a
  lock: [RFC 0063](0063-per-owner-write-serialization.md) maps it to a transaction-scoped advisory
  lock on Postgres, and a backend with another mechanism may use that instead.

Members are added by demand and each one costs a capability flag, a per-adapter mapping and a
mock implementation — which is the intended brake on the vocabulary growing into an algebra.

### 5.2 Where a guarantee is declared

On the spec, in one field, typed as the guarantee tuple:

```python
DocumentSpec(..., guarantees=(UniqueTogether(fields=("root_id",), where=...),))
```

`DocumentSpec` first, because that is where the demand is — but the vocabulary lives in
`contracts/guarantees/` and names no plane, so `GraphSpec` or a counter spec can take the same
field without a second vocabulary. A guarantee that a plane cannot express (a period on a counter)
is caught by the same reconciliation as one a backend cannot enforce, so the vocabulary does not
need a per-plane allow-list.

### 5.3 Reconciliation: the declared half

Each participating adapter declares what it can enforce, in the shape its plane already uses:

```python
StorageGuaranteeCapabilities(
    unique_together=True,
    unique_together_filtered=True,   # a partial/filtered unique constraint
    non_overlapping=True,
    serialized_by=True,
)
```

`check_wiring` reconciles every spec's guarantees against the resolved adapter's set and reports
one finding per unsatisfiable guarantee, naming the guarantee, the spec and the backend — the same
sentence shape `query_feature_unsupported` uses, for the same reason: a caller must be able to
tell "this backend cannot do that" from "something broke".

**An unsatisfiable guarantee is a refusal, never a downgrade.** A silent skip turns a declared
property into a comment, which is the failure mode all three source kits were trying to escape.

### 5.4 Validation: the physical half

Reconciliation says the backend *can*; validation says the deployment *did*. At startup, each
adapter checks its own mechanism and refuses with the DDL that would satisfy it — Postgres inside
`validate_postgres_document_schemas` against `pg_indexes` and `pg_constraint`, Mongo where it
already lists indexes.

**Declared, validated, never created.** An adapter that created an index would take a lock on a
production table nobody asked it to take, and the one that fixed it silently would hide a missing
migration until the next deployment.

### 5.5 The mock is the superset

The in-memory store enforces every guarantee, exactly as `FULL_QUERY_CAPABILITIES` makes the mock
the canonical superset for filters. This is what makes a guarantee testable under simulation
rather than only in an integration environment — and it is the leg most easily got wrong in the
optimistic direction: a mock that enforced *more* than a backend would pass a simulation that a
deployment fails, so the parity battery (§6) compares the two.

### 5.6 Guarantees and `SystemInvariant` are different tools

| | `SystemInvariant` | Storage guarantee |
| --- | --- | --- |
| Declares | a law over a read-set, reduced to one number | a property the store refuses to violate |
| Enforced by | the framework, in or after the writing transaction | the store, at write time |
| Detects | a violation that already happened (detective) or prevents it under isolation | prevents it, always |
| Expresses | anything `SumOf`/`CountAll` can score | uniqueness, non-overlap, serialization |

They overlap on exactly one case — `single_current_head` is expressible both ways — and that is
useful rather than redundant: the invariant is what a backend without the guarantee falls back to,
and it is what a simulation asserts about the kit's own behaviour.

### Alternatives considered

- **A private mechanism per kit** (the drafts as written). Three vocabularies, three validators,
  three answers to the same question, and the first one already leaked DDL into contracts.
- **Extend `BackendRequirements`.** It is a route checklist that explicitly declines to describe
  backend config, so extending it would contradict its own docstring.
- **Let the adapter's config declare the index** (`PostgresDocumentConfig(unique_indexes=…)`).
  Honest about layering and it puts a *correctness* requirement in a per-deployment config, where a
  kit cannot see it and a second deployment can forget it.
- **Create the DDL.** Removes the missing-migration failure entirely, and makes the framework take
  locks on production tables. §5.4.
- **Reuse a capability flag per guarantee instead of a set** (`supports_partial_unique`). Matches
  the existing style closely; a set keeps one place to look when the vocabulary grows.

## 6. Tests

- Reconciliation: a spec declaring each guarantee against an adapter that lacks it produces one
  `check_wiring` finding naming guarantee, spec and backend; against an adapter that has it, none.
- No silent downgrade: the unsatisfiable case never resolves — asserted by resolving the operation,
  not by reading the report.
- Validation: with the mechanism dropped from a live Postgres fixture, startup refuses and the
  message contains the DDL; with it present, startup passes. The Mongo equivalent for
  `UniqueTogether` with a partial filter.
- Mock enforcement, per guarantee: a violating write raises `conflict`, and the same case raises
  `conflict` against Postgres — the **parity leg**, because §5.5's risk is a mock that is stricter
  than the backend.
- `UniqueTogether` with `where`: a row violating the tuple *outside* the filter is accepted; inside
  it, refused.
- `NonOverlapping` under both bounds conventions, reusing [RFC 0067](0067-period-and-overlap.md)'s
  edge cases so the guarantee and the predicate cannot disagree.
- `SerializedBy`: two concurrent writes for one key do not interleave; two for different keys do
  (the second leg, or a global lock would pass the first).
- DST: each guarantee's property as an invariant over a concurrent workload, with the same spec
  minus the guarantee as the ungoverned contrast — a contrast that must fail.
- **Not tested:** that a partial unique index or an exclusion constraint works. Those are the
  database's properties; the batteries assert that the refusal reaches the caller as `conflict` and
  that a missing mechanism is a startup refusal.

## 7. Docs

A page section under the aggregates/specs reference: the three members, the per-backend support
table (which is generated from the capability declarations, so it cannot drift into fiction), and
the two sentences that carry the doctrine — **a guarantee is declared, validated and never
created**, and **your migration is the thing that satisfies it**. Plus the comparison table from
§5.6, because "should this be an invariant or a guarantee" is the question every author will ask.

## 8. Out of scope

- **Further members** (`Monotonic`, `ReferencesExisting`, `MaxRows`). Each needs a consumer, a
  capability flag and a mock implementation; the vocabulary grows by demand.
- **Guarantees on non-document planes.** The vocabulary is plane-neutral (§5.2) and only the
  document plane implements it here. Graph edge uniqueness is the most likely second consumer.
- **A generated migration.** Naming what would satisfy a guarantee is in scope; emitting a
  migration file is a tool, not a contract.
- **Unifying the nine capability declarations.** §4.
- **Tenant-scoped guarantees.** The tenancy floor already scopes writes; whether a guarantee is
  per-tenant or global is the adapter's mapping, and stating it per member is the first thing to
  add if a consumer needs both.

## 9. Risks

- **A vocabulary that grows one member per kit.** The brake is the cost (flag + mapping + mock +
  parity leg) and §8's rule that a member needs a consumer. The failure mode is a half-implemented
  member, which the reconciliation refuses loudly rather than skipping.
- **The mock as an optimistic oracle.** §5.5 and the parity leg; a mock stricter than a backend
  makes simulations pass where deployments fail.
- **A new class of boot failure.** Deployments that run today can refuse to start once a kit
  declares a guarantee they never migrated for — intended, and still a break. Mitigation: findings
  reach `check_wiring` first, so CI sees them before production does.
- **Read as schema management.** "Declared guarantees" sounds adjacent to migrations, and the
  framework is not offering any. Mitigation: the doctrine sentence in §7 and decision 3.
- **Two mechanisms for one property.** With `SystemInvariant` also able to express uniqueness, an
  author can declare both and pay twice. §5.6 is written to make the choice obvious, and declaring
  both is legitimate where the guarantee is the prevention and the invariant is the proof.

## 10. Unresolved questions

- **Do findings arrive as a new `WiringReport` field or as `WiringFailure`s?** Shared with
  [RFC 0061](0061-production-posture.md), which has the same question for posture violations, and
  the two should answer it once: the report has no findings channel today, and a guarantee
  mismatch is not an operation that failed to resolve.
- **Is the capability declaration a set per adapter or flags on the existing plane capabilities?**
  A `StorageGuaranteeCapabilities` value keeps guarantees in one place; folding the flags into
  `QueryCapabilities`-style declarations keeps one object per plane. Leaning: its own value,
  because a guarantee is not a query feature.
- **Does `SerializedBy` belong here at all?** It is the one member that is about *write ordering*
  rather than a property of stored rows, and [RFC 0063](0063-per-owner-write-serialization.md)
  could keep it as its own spec field. Leaning: here, because the reconciliation and refusal
  machinery is identical and a second mechanism for one class of thing is what this RFC exists to
  prevent.
- **Per-tenant versus global uniqueness.** Today's answer is the adapter's mapping; if two
  consumers disagree it becomes a member field.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | A guarantee declares a **property**, never a mechanism. No index, constraint, extension, lock or range type appears in the vocabulary — `forze.application.contracts` must not know what `gist` is, and the first draft of this vocabulary proved how easily it does. |
| 2 | `LOCKED` | Guarantees are the **declared half of the shipped capability convention**: adapter publishes what it can enforce, wiring reconciles, refusal names feature and backend, mock is the canonical superset. A parallel mechanism would be the tenth way to say the same thing. |
| 3 | `LOCKED` | **Declared, validated, never created.** An adapter that created an index would take a production lock nobody asked for, and would hide the missing migration. |
| 4 | `LOCKED` | An unsatisfiable guarantee **refuses at wiring**; there is no downgrade and no warning-only mode. A skipped guarantee turns a declared property into a comment. |
| 5 | `LOCKED` | The mock enforces every guarantee, and a **parity battery** compares mock and backend refusals. A mock stricter than the backend makes a green simulation meaningless. |
| 6 | `ASSUMED` | The vocabulary is plane-neutral and lives in `contracts/guarantees/`; the document plane implements it first. A per-plane vocabulary is how three kits ended up with three. |
| 7 | `ASSUMED` | `SystemInvariant` is not merged with guarantees. One is a law the framework checks over a read-set, the other is a property the store refuses; §5.6's table is shipped in the docs because authors will ask. |
| 8 | `ASSUMED` | Members are added by demand only, each paying for a capability flag, a per-adapter mapping, a mock implementation and a parity leg. |
| 9 | `OPEN` | Whether findings arrive as a `WiringReport` field or synthesized failures (shared with [RFC 0061](0061-production-posture.md)), whether the capability declaration is its own value or flags on existing ones, and whether `SerializedBy` belongs in this vocabulary. |

## 12. Phasing

- **P1** — the vocabulary, the spec field, the capability declaration, `check_wiring`
  reconciliation, the mock's enforcement of `UniqueTogether`, and the batteries. Unblocks
  [RFC 0052](0052-versioned-facts-correction-lineage.md).
- **P2** — the Postgres and Mongo mappings and startup validation for `UniqueTogether`, with the
  parity legs.
- **P3** — `NonOverlapping` (needs [RFC 0067](0067-period-and-overlap.md)) and its Postgres
  mapping. Unblocks [RFC 0053](0053-temporal-validity.md).
- **P4** — `SerializedBy`, if decision 9 lands that way. Unblocks
  [RFC 0063](0063-per-owner-write-serialization.md)'s hard-guarantee half.
- **P5** *(demand-gated)* — a second plane, further members, the generated support table in docs.
