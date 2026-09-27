# T-guarantee-holds — a guarantee can say when it must hold

Executing the maintainer's specification (a `holds` moment on `UniqueTogether` and
`NonOverlapping`, and the refusal of a deferred constraint behind an immediate guarantee) on
`feat/guarantee-holds-at-commit`. The specification carries no graded decision table, so its
decisions are treated as `LOCKED`.

## Plan

- `contracts/guarantees/value_objects.py`: `holds: Literal["always", "commit"] = "always"` on
  `UniqueTogether` and `NonOverlapping`, refused at construction when it is anything else; not
  on `SerializedBy`.
- `contracts/guarantees/capabilities.py`: `checks_deferred_to_commit` (the specification's flag)
  and `unique_together_partial_at_commit`, both `False` by default; `unmet` consults them only
  where a guarantee asks; `FULL_STORAGE_GUARANTEES` sets both.
- `forze_mock`: the eager check skips a `holds="commit"` guarantee inside a transaction and keeps
  it outside one; the commit recheck (unchanged in what it checks) stops skipping a namespace
  that has nothing committed yet.
- `forze_postgres`: the adapter declares `checks_deferred_to_commit`; the introspector reads
  `condeferred` for unique indexes and exclusion constraints; startup matches deferral against
  `holds`, refuses a mismatch naming the deferral, and prints constraint DDL carrying
  `DEFERRABLE INITIALLY DEFERRED` for a commit guarantee.
- Batteries: the vocabulary and the reconciliation (unit); the mock's transaction legs; the
  Postgres startup legs; mock-and-Postgres parity for a transaction that passes through a
  violation.
- Docs (the guarantees section of the document-port reference) and the changelog.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-09-27T10:45:00Z
attempt: 1
claim: the specification refuses `holds="commit"` with `where=` "on Postgres" in general, reasoning from uniqueness — a unique constraint cannot be partial; the same does not hold for non-overlap, whose mechanism is an EXCLUDE constraint, which Postgres allows to be both partial (`WHERE`) and `DEFERRABLE INITIALLY DEFERRED`
evidence: src/forze_postgres/kernel/catalog/validation/validate_schema.py:521
action: decided
proposal: ASSUMED — the refusal applies to `UniqueTogether` only. A filtered `NonOverlapping` holding at commit is accepted on Postgres and validated against a deferred partial EXCLUDE constraint — the specification's own rule that a refusal must not forbid a declaration the backend can keep.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-09-27T10:45:00Z
attempt: 1
claim: `skip_null` is kept on Postgres by a partial unique index — startup deliberately refuses a plain unique index for it, and a battery pins that — so a `skip_null` uniqueness holding at commit needs a partial index that cannot be deferred: the same impossibility as `where`, which the specification does not mention
evidence: tests/integration/test_forze_postgres/test_pg_document_guarantees.py:695
action: decided
proposal: ASSUMED — one capability flag, `unique_together_partial_at_commit`, covers both partial forms (`where` or `skip_null`) of a uniqueness holding at commit; Postgres leaves it off and the mock sets it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-09-27T10:45:00Z
attempt: 1
claim: deliverable 1 names unique indexes; the same defect holds for non-overlap — an EXCLUDE constraint declared `DEFERRABLE INITIALLY DEFERRED` today satisfies an immediate `NonOverlapping`, which the mock enforces per write
evidence: src/forze_postgres/kernel/catalog/introspect/introspector.py:654
action: decided
proposal: ASSUMED — both members match deferral against `holds`, and both refusals name the deferral.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-09-27T10:45:00Z
attempt: 1
claim: `pg_constraint.conindid` is also set on a foreign key in another table that references a unique index, so the specification's `LEFT JOIN pg_constraint ON conindid = i.indexrelid` would let a deferred foreign key make the referenced index read as deferred, or duplicate its row
evidence: src/forze_postgres/kernel/catalog/introspect/introspector.py:557
action: decided
proposal: the join is restricted to the index's own relation and to unique, primary-key and exclusion constraints; a battery leg puts a deferred foreign key on another table and expects the index to read as immediate.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-09-27T10:45:00Z
attempt: 1
claim: the commit recheck skips a namespace with nothing committed (`if not live: continue`), which is sound only while the eager check has already covered the transaction's own writes; a `holds="commit"` guarantee skips that eager check, so two violating rows written in the first transaction to touch a namespace would commit unchecked
evidence: src/forze_mock/adapters/_mvcc.py:366
action: decided
proposal: the recheck no longer skips an empty namespace; the published view it judges is the transaction's overlay over whatever is committed, empty included. A leg covers the first transaction into an empty store.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-09-27T10:45:00Z
attempt: 1
claim: the specification says Mongo "already refuses guarantees outright"; it keeps `UniqueTogether` (unfiltered, filtered, `skip_null`) through unique indexes, which it checks per write and cannot defer
evidence: src/forze_mongo/adapters/document.py:57
action: decided
proposal: nothing to add: a new flag that defaults to `False` refuses `holds="commit"` on Mongo at wiring, which is the specification's intent reached by its own rule that silence is not consent.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-09-27T10:45:00Z
attempt: 1
claim: `pg_get_constraintdef` renders a deferred EXCLUDE constraint with its characteristics appended (`... WHERE (...) DEFERRABLE INITIALLY DEFERRED`), and the predicate is read off that text, so the deferral clause would be read as part of the predicate
evidence: src/forze_postgres/kernel/catalog/validation/validate_schema.py:486
action: decided
proposal: the constraint characteristics are stripped before the predicate is read; deferral itself is read structurally from `condeferred`, never from the text.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-09-27T10:45:00Z
attempt: 1
claim: `holds` is a `Literal` the runtime does not enforce; a declaration read from configuration with `holds="Commit"` or `"deferred"` would compare unequal to `"commit"` everywhere and quietly behave as `"always"`
evidence: src/forze/application/contracts/guarantees/value_objects.py:48
action: decided
proposal: ASSUMED — `holds` outside the two values is refused at construction as `configuration`.
```

**Drift count: 0**

## Self-audit, round 1 — 2026-09-27

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-09-27T11:40:00Z
attempt: 2
claim: the earlier entry's stripping of the deferral clause before the predicate is read changes nothing observable — `_predicate_names` matches identifiers case-sensitively, Postgres folds an unquoted identifier to lower case and renders the clause in upper case, so the clause can collide only with a quoted all-capitals column named `"DEFERRED"`; sabotage removing the strip survived a leg written to kill it, which is the proof
evidence: src/forze_postgres/kernel/catalog/validation/validate_schema.py:423
action: decided
proposal: the strip and its leg are removed rather than kept as code that cannot be shown to do anything; deferral is still read from `condeferred`, never from the definition text.
```

**Drift count: 0**

Findings, round 1 (scope: the branch — vocabulary, reconciliation, mock enforcement, Postgres
introspection and startup, docs, changelog):

1. **Fixed — code that did nothing.** The deferral-clause strip (entry above): sabotage removing
   it survived a leg written to kill it. Removed, with the leg.
2. **Fixed — an untested path.** The set-based update (`update_matching`) validates a staged
   batch through its own call, and nothing exercised its deferral: a mutant making it never
   defer survived the battery. A leg now drives a bulk update through a transient duplicate
   inside a transaction.

Checks run: sabotage, 17 mutants — 16 killed by an assertion (mock eager check both ways, the
empty-namespace recheck, every reconciliation arm, the moment validation, the foreign-key join,
deferral read and compared for both members, both DDL forms, the partial-at-commit refusal, the
adapter flag), the 17th the equivalent mutant above. Patch coverage 100% (68/68 added source
lines) over the unit suites and the Postgres guarantee file. The specification's test list is
covered point by point, the parity legs comparing the two stores in one test.

**Drift count: 0**
