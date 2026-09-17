# RFC 0052 — Versioned facts with correction lineage

- **Status:** 📝 Draft — execution-ready, and the head of the batch: `0053` and `0063` both wait on the backend-requirement primitive this RFC owns (§5.4).
- **Scope:** A `forze_kits.aggregates.versioned` kit that turns "correct, never overwrite" into one declaration: lineage fields on the domain model, a `correct` command that supersedes in one transaction, a read side that defaults to the current row, a declared backend constraint, and one `SystemInvariant` the DST oracle already knows how to compile. Touches `forze_kits` plus **one new contract field** for the declared storage guarantee (§5.4) — a property, not DDL — and its Postgres/Mongo/mock implementations. No change to the document ports, the history relation, or `rev`.
- **Related:** [`src/forze_kits/aggregates/soft_deletion/`](../src/forze_kits/aggregates/soft_deletion/) (the kit shape this copies: `factories.py`, `handlers.py`, `operations.py`, `wiring.py`, and `DELETE`/`RESTORE` as the op-key precedent), [`src/forze_kits/domain/soft_deletion/`](../src/forze_kits/domain/soft_deletion/) (the mixin shape), [`src/forze_kits/aggregates/kit.py:146`](../src/forze_kits/aggregates/kit.py) (`AggregateKit`'s config fields), [`src/forze/domain/models/document.py:460`](../src/forze/domain/models/document.py) (`DocumentHistory`), [`src/forze/application/contracts/invariants.py`](../src/forze/application/contracts/invariants.py) (`SystemInvariant`, `ReadSet`, `CountAll`), [`src/forze_postgres/kernel/catalog/validation/validate_schema.py:266`](../src/forze_postgres/kernel/catalog/validation/validate_schema.py) (`validate_postgres_document_schemas`, the startup refusal this extends), [`src/forze_kits/aggregates/kit.py:103`](../src/forze_kits/aggregates/kit.py) (the shipped `BackendRequirements` wiring checklist, which §5.4 is *not*), [`src/forze_mongo/kernel/validate_indexes.py:46`](../src/forze_mongo/kernel/validate_indexes.py) (`validate_mongo_document_indexes`, where Mongo's half lands), RFC 0013 (`AggregateKit`, shipped and retired from this directory — the precedent for one declaration standing in for a hand-written slice). Sibling RFCs from the same source: [0053](0053-temporal-validity.md), [0055](0055-scoped-disclosure.md), [0063](0063-per-owner-write-serialization.md).
- **Origin:** A working-time ledger (Chronobeam) where seven fact tables carry `version` / `supersedes_id` / a `correction` row, hand-rolled 14 times for the "is current" anti-join, 7 times for chain walkers and 9 times for the conflict check. Two of the audit's high findings live in exactly that hand-rolled code: one anti-join compiles to a dead predicate, and no unique index on `supersedes_id` exists anywhere, so two concurrent corrections both win.

---

## 1. Summary

An aggregate declares `versioned=VersionedPolicy(...)` on its kit. Its domain model gains
`root_id`, `version`, `supersedes_id`, `is_current` and `superseded_at`; a `correct` operation
inserts the successor, clears `is_current` on the predecessor and writes a `Correction` record
naming the actor and a reason code, in one transaction; every generated read filters
`is_current = true` unless the caller asks for the chain. The kit declares two storage
guarantees and one invariant, and the invariant compiles into the DST oracle that already
exists for `SystemInvariant`.

## 2. Motivation

A regulated ledger is not allowed to overwrite a fact. Payroll, medical records, invoices and
audit trails all want the same three properties: the old value stays readable, the new value
says what it replaced, and somebody's name is on the change.

The origin application needed that and had no primitive for it, so it wrote the pattern per
table. The measured cost was not the fields — it was the *derived* half:

- **"Current" as an anti-join.** `NOT EXISTS (SELECT 1 FROM t s WHERE s.supersedes_id = t.id)`
  appears 14 times. One of them compares the wrong column and compiles to a predicate that is
  always true, so a corrected row is served as current. Nothing failed; the read was simply
  wrong, in the product's central read path.
- **No uniqueness on the pointer.** With no unique index on `supersedes_id`, two concurrent
  corrections of one fact both insert successors and both commit. The chain forks, and every
  chain walker then picks whichever row it saw first.

Both defects are in code a kit would have replaced, which is the argument for building it here
rather than documenting the pattern.

## 3. Current state

**`rev` and the history relation give snapshots, not lineage.** `DocumentSpec.history_enabled`
writes a `DocumentHistory` row per revision, and that row is
`(source, id, rev, created_at, data)` — verified at
[`document.py:460-476`](../src/forze/domain/models/document.py). There is no actor, no reason,
no predecessor pointer, and no way to tell "same fact, edited" from "new version of the fact".
A history row is also not addressable as a fact: nothing can reference revision 3 of a row.

**The kit shape exists and is proven.** `soft_deletion` is the same class of feature — extra
fields on the model, a write-side operation pair, a read-side filter, wiring that merges both
into a generated registry — and it ships as four modules plus a domain mixin package.
`AggregateKit` already composes `soft_delete`, `search`, `invariants`, `outbox` and `storage`
behind one config, so a fifth arm costs a field and a wiring call.

**The invariant machinery covers half of what this needs.** `SystemInvariant` is a named
`ReadSet` (spec + `scope_keys` + constant `where`) reduced by a `Reducer` to one number, with
`holds(value) -> bool` and a `required_isolation` floor; `forze_kits.invariants.enforce_preventive`
runs it inside the writing transaction and `forze_dst.oracle.system_invariants.compile_oracle`
turns the same declaration into a simulation oracle. **`Reducer` is a closed set** —
`SumOf | CountAll` — because each member has to both push down to the query port and fold from a
recorded trace. So `single_current_head` is expressible today and `lineage_closed` is not (§5.5).

**No storage guarantee can be declared.** Postgres validates *columns* at startup
(`validate_postgres_document_schemas` raises `internal` for a model field with no column) and
Mongo lists indexes and warns. Neither takes a declared requirement — a unique index, a partial
index, an exclusion constraint — so a kit that depends on one for correctness has no way to
fail at boot when the deployment forgot the migration. `forze_kits.aggregates.BackendRequirements`
([`kit.py:103`](../src/forze_kits/aggregates/kit.py)) is the nearest shipped thing and is a
different one: a checklist of *routes* to wire, explicitly declining to describe backend config.

## 4. Goals / Non-goals

**Goals**

- One declaration adds the fields, the correction command, the read filter and the invariant.
- "Current" is a **stored flag**, not a derived anti-join: the flag is what an index and an
  invariant can see.
- A correction names its actor and its reason, and both are part of the record.
- The deployment's missing index is a boot refusal, not a race in production.

**Non-goals**

- **Not bitemporal.** Validity over time is [RFC 0053](0053-temporal-validity.md)'s subject;
  this RFC versions *assertions about a fact*, not the fact's effective period. The two compose
  (§5.6) and stay separate declarations.
- **Not an audit log.** A `Correction` records one aggregate's lineage. Cross-aggregate
  who-did-what is [RFC 0060](0060-audit-spec.md).
- **Not a replacement for `history_enabled`.** History stays what it is: a per-revision snapshot
  of the row. A versioned aggregate may enable both; they answer different questions.
- **Not an approval workflow.** A correction applies immediately. Proposal-then-acceptance is
  [RFC 0039](0039-proposal-lifecycle.md).

## 5. Design

### 5.1 The mixins

```python
class VersionedMixin(CoreModel):
    root_id: UUID = Field(frozen=True)          # the fact's identity across versions
    version: int = Field(frozen=True, ge=1)
    supersedes_id: UUID | None = Field(default=None, frozen=True)
    is_current: bool = True
    superseded_at: datetime | None = None
```

`root_id` is the fact; `id` is this version of it. A first insert sets `root_id = id`,
`version = 1`, `supersedes_id = None`. Mirrors `forze_kits.domain.soft_deletion`: a
`DocWithVersioning` / `UpdateCmdWithVersioning` pair so the kit's type preconditions are checked
by the type system rather than at runtime.

### 5.2 The correction command

```python
correct(id: UUID, expected_version: int, patch: U, reason: str, ...) -> R
```

One transaction, in this order: read the predecessor and refuse unless it is current and at
`expected_version` (`conflict`, so the boundary renders 409); insert the successor carrying the
patch, `version + 1` and `supersedes_id = id`; update the predecessor to
`is_current = False, superseded_at = now`; insert a `Correction` document
`(root_id, from_id, to_id, actor_id, reason, at)`. The actor is read from `ctx.inv_ctx`, never
taken from the caller's payload.

`expected_version` rather than `rev`: a caller correcting a fact is asserting *which version of
the fact* it read, which survives an unrelated column update that bumped `rev`.

### 5.3 The read side

The generated LIST family merges `is_current = true` into every filter and GET 404s a
superseded row, exactly as `soft_deletion`'s read mappers do
([`wiring.py`](../src/forze_kits/aggregates/soft_deletion/wiring.py) — mappers applied at build
time, ops merged after). Two additions on the facade:

- `history(root_id)` — the chain in version order, oldest first.
- `as_of(root_id, at)` — the version current at an instant, from `created_at` /
  `superseded_at`. Named here because a ledger asks for it constantly; it reads only stored
  columns, so it is not a temporal-validity feature.

### 5.4 The declared storage guarantee

A new field on `DocumentSpec`, and the vocabulary is a **property**, never DDL:

```python
guarantees: tuple[StorageGuarantee, ...] = ()

UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
UniqueTogether(fields=("supersedes_id",), skip_null=True)
```

`where` is an ordinary `QueryFilterExpression` — the filter language is already backend-agnostic
and every adapter parses it — so the declaration says *at most one row per `root_id` among the
rows matching this predicate* and names no index, no partial-index syntax and no extension.
`forze.application.contracts` cannot spell `gist`, and nothing in it should have to.

Each adapter maps the guarantee to whatever it has, and **validates, never creates**: a framework
that silently creates an index silently takes a lock on a production table.

| Backend | Mechanism | On mismatch |
| --- | --- | --- |
| Postgres | partial unique index, checked against `pg_indexes` inside `validate_postgres_document_schemas` | `internal` at startup, naming the guarantee and the DDL that satisfies it |
| Mongo | unique index with `partialFilterExpression`, checked where indexes are already listed | same refusal, from the mechanism that already warns about secondary unique indexes |
| Mock | in-memory enforcement, so DST sees the guarantee rather than assuming it | a violated write raises `conflict`, as a real backend would |

A backend with no mechanism for a guarantee **refuses the declaration at wiring** rather than
skipping it silently — the rule [RFC 0053](0053-temporal-validity.md) decision 2 states for its
own case. `UniqueTogether` happens to be portable across all three; the non-overlap guarantee
0053 needs is not, which is exactly why the vocabulary is per-guarantee and the capability answer
is per adapter.

**Not to be confused with the shipped `BackendRequirements`**
([`kit.py:103`](../src/forze_kits/aggregates/kit.py)), which is a *wiring* checklist — routes, a
tx route, whether a keyring is needed — and deliberately fabricates no backend config. A storage
guarantee is a property of the aggregate's data, so it belongs on the spec, which is usable
without a kit; the checklist stays what it is.

### 5.5 The invariants, and which one the oracle can take

`single_current_head` is expressible with what exists:

```python
SystemInvariant(
    name="single_current_head",
    read_set=ReadSet(spec=spec, scope_keys=("root_id",), where={"$values": {"is_current": True}}),
    aggregate=CountAll(),
    holds=lambda n: n <= 1,
)
```

`lineage_closed` — every non-null `supersedes_id` resolves to a row of the same `root_id` — is
**not** expressible: it is an existence check across rows, and the reducer set is closed at
`SumOf | CountAll` for a reason (each member folds from a recorded trace as well as pushing
down). Three ways out, and this RFC takes the third:

1. A new reducer. Rejected: a join-shaped reducer has to fold from a trace too, and nothing else
   asks for one yet.
2. A `forze_dst` history invariant. Possible, and it would only cover simulated runs.
3. **The `UniqueTogether` guarantee on `supersedes_id` plus the command's own transaction.** A
   fork requires two successors pointing at one predecessor, which the guarantee refuses, and an orphan requires the
   successor to commit without its predecessor's update, which the single transaction refuses.
   The property is enforced by construction, so it is documented as a constraint rather than
   declared as a law.

### 5.6 Composition with 0053

A re-versioned contract wants both: lineage (who corrected the assertion) and validity (which
period it applies to). The two kits write disjoint field sets and disjoint filters, so
composition is "declare both"; the one interaction is that `0053`'s non-overlap constraint must
be scoped to current rows, which its exclusion predicate states.

### Alternatives considered

- **Keep the anti-join, drop `is_current`.** One less field and no update on the predecessor —
  and the whole failure the origin application hit: an anti-join is invisible to an index and to
  an invariant, both of which read a column. The flag is redundant state on purpose.
- **Version in place, keep one row.** `version` bumps and the old value goes to the history
  relation. Cheapest, and it loses addressability: nothing can reference the superseded fact,
  which a disclosure grant ([RFC 0055](0055-scoped-disclosure.md)) has to do.
- **A `Correction` as an event rather than a document.** The outbox already carries events, and
  an event is not queryable — "show me every correction to this fact with its reason" is the
  question a ledger is audited on.

## 6. Tests

- **Battery per behaviour, over the mock and Postgres:** first insert seeds `root_id = id`;
  `correct` inserts, flips, records; a stale `expected_version` is `conflict`; a correction of a
  superseded row is `conflict`; the read family hides superseded rows; `history` returns the
  chain in order; `as_of` picks the right version at boundaries (equal to `created_at`, equal to
  `superseded_at`).
- **DST:** two concurrent `correct`s on one fact under the kit's composed registry, with
  `single_current_head` as the oracle and a bare registry as the ungoverned contrast — the
  shape `examples/recipes/aggregate_kit_dst/` already proves for a capacity cap.
- **The declared requirement:** a Postgres fixture with the index dropped must fail at startup
  naming the index; the same case on the mock must fail in DST.
- **Not tested:** that the index makes a fork impossible — that is the database's property, not
  ours. The battery asserts the refusal is surfaced as `conflict`.

## 7. Docs

A section in the aggregates/kits page: the declaration, the one screen of generated surface,
and the sentence that matters — **"current" is a stored flag, and the index that protects it is
your migration to write.** The `guarantees` vocabulary gets its own short page section with the
per-backend table, since 0053 and 0063 point at it.

## 8. Out of scope

- **Merging two chains** (the same fact entered twice, discovered later). Named because a ledger
  eventually wants it; it needs a policy this RFC has no basis to pick.
- **Bulk correction.** The command is one fact at a time. What would change it: a named consumer
  correcting a month of rows in one call, where the per-row transaction is the cost.
- **Reason codes as a closed vocabulary.** `reason` is a string here. An app that wants an enum
  declares one; a framework-owned list would be wrong for every domain.

## 9. Risks

- **Two sources of truth for "current".** The flag and the chain can disagree if anything writes
  outside the command. Mitigation: the flag is only written by the kit's own handlers, and
  `single_current_head` is the detective control that catches a path that bypassed them.
- **A declared requirement nobody satisfies becomes a boot failure in production.** That is the
  intent, and it is still a new way to fail at deploy time. Mitigation: the refusal names the
  exact DDL, and `check_wiring` reports it before `build_runtime` does.
- **Read as an audit trail.** A `Correction` says who changed a fact, which invites the reading
  that the kit is compliance-complete. The docs say plainly that it covers one aggregate's
  lineage and nothing else.

## 10. Unresolved questions

- **Does `correct` emit a domain event by default?** The outbox arm is already composed by
  `AggregateKit`; a correction is exactly the kind of fact downstream consumers want. Settled by
  whether the first consumer is inside the app or across a boundary.
- **Is `as_of` on the facade or a query option?** A facade method is discoverable; a query option
  composes with filters. Implementation decides, and the row records it.
- **Does the guarantee vocabulary live on `DocumentSpec` or beside it?** `DocumentSpec` is
  already wide (§3 lists fifteen fields). One `StorageGuarantees` value object mounted as a single
  field keeps it one field, and that is the leaning — and it keeps the name clear of the shipped
  `BackendRequirements`, which is a wiring checklist and not this.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | "Current" is a **stored `is_current` flag**, never a derived anti-join. The flag is what an index and a `SystemInvariant` can read; the anti-join is what the origin application got wrong twice. Consequence: every write path must go through the kit's handlers, and the invariant is the control that catches one that did not. |
| 2 | `LOCKED` | A correction is a **new row plus a `Correction` document in one transaction**, never an update in place. Addressability is the point: a disclosure grant, an export and a citation all have to name the version they saw. |
| 3 | `LOCKED` | The spec **declares** its storage guarantees and the adapters **validate** them at startup; nothing creates DDL. A framework that creates an index takes a lock on a production table nobody asked it to take. |
| 4 | `ASSUMED` | `lineage_closed` is enforced **by construction** (the unique index on `supersedes_id` plus the single transaction) rather than declared as a law, because the reducer set is closed at `SumOf \| CountAll` and nothing else yet needs a join-shaped reducer. Depart if a second consumer appears. |
| 5 | `ASSUMED` | `correct` takes `expected_version`, not `rev`: the caller asserts which version of the fact it read, which an unrelated column update must not invalidate. |
| 6 | `LOCKED` | The vocabulary declares a **property** (`UniqueTogether(fields, where=<filter expression>)`), never a backend's DDL. `DocumentSpec` lives in `forze.application.contracts`, which must not spell `gist`, `partialFilterExpression` or `nulls distinct`; each adapter maps the property to its own mechanism, and one with no mechanism refuses at wiring. |
| 7 | `ASSUMED` | Guarantees land on `DocumentSpec` (a property of the aggregate's data, usable without a kit), not on the kit's shipped `BackendRequirements`, which is a wiring checklist that deliberately fabricates no backend config. Whether they arrive as a tuple field or one mounted value object is implementation's. |
| 8 | `OPEN` | Whether `correct` emits a domain event by default, always, or on declaration. Settled by the first consumer. |

## 12. Phasing

- **P1** — mixins, `correct`, the read side, `history` / `as_of`, the invariant, mock + Postgres
  batteries, the DST leg. No backend requirement: the invariant is the control.
- **P2** — the `guarantees` vocabulary with `UniqueTogether`, its Postgres, Mongo and mock
  implementations, and the wiring refusal for a backend without a mechanism. Unblocks
  [0053](0053-temporal-validity.md) and [0063](0063-per-owner-write-serialization.md).
- **P3** *(demand-gated)* — `as_of` as a query option, bulk correction, chain merge. Each needs a
  named consumer.
