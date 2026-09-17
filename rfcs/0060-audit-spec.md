# RFC 0060 — Audit spec with allowlisted metadata

- **Status:** 📝 Draft — execution-ready. [RFC 0055](0055-scoped-disclosure.md) waits on it for audit-on-read.
- **Scope:** An `AuditPort` with one method, an `AuditSpec` whose **metadata allowlist is the point**, a `@audited` operation decorator that binds the actor from the invocation context, and a document-spec-backed adapter. Adds a contract package (`contracts/audit/`) and a kit-level wiring arm; the Postgres/Mongo/mock adapters are one `DocumentSpec` each. No change to the operation plan's hook vocabulary — §3 shows the hooks needed are already there.
- **Related:** [`src/forze/application/contracts/execution/protocols.py:30-70`](../src/forze/application/contracts/execution/protocols.py) (`Before`, `OnSuccess`, `OnFailure`, `Finally` — and `Finally`'s "always runs once the scope is entered, including when a `before` hook raises", which is what lets a denial be audited), [`src/forze/application/execution/operations/run/plan.py:89`](../src/forze/application/execution/operations/run/plan.py) (the comment naming audit as a thing that should see guard denials), [`src/forze/base/scrubbing/`](../src/forze/base/scrubbing/) (`sanitize`, `is_sensitive_key` — scrubbing exists and is **not** what an allowlist is, §5.3), [`src/forze_kits/aggregates/kit.py`](../src/forze_kits/aggregates/kit.py) (where the wiring arm goes), [RFC 0055](0055-scoped-disclosure.md) (audit-on-read after authorization), [RFC 0057](0057-derived-capabilities-and-config-grants.md) (a derived grant is attributable, so a decision is auditable).
- **Origin:** A working-time ledger with one `audit_event` table (actor, action, object type and id, subject id, outcome, metadata JSONB) and a `record_audit` that **raises** on any metadata key outside a frozen allowlist — so a comment, a reason, or a request body can never land in the audit log. Reads are audited only after authorization succeeds; GETs of one's own data are never audited.

---

## 1. Summary

An app declares which actions are audited and, per action, which metadata keys may be recorded.
Anything else raises at the call site. The actor comes from the invocation context, never from a
payload; the outcome is recorded for denials as well as successes; and the adapter is an ordinary
document spec, so the audit trail is queryable, exportable and tenant-scoped like everything
else.

## 2. Motivation

Forze has no audit port, so every app that needs one builds it — and the failure mode is not
"forgot to log", it is **logging too much**. An audit trail assembled from request bodies becomes
a second copy of the personal data it exists to protect: unencrypted, long-retained, widely
readable, and outside every field-encryption and scrubbing policy the app carefully applied to the
primary store.

The allowlist is the part worth shipping. A table is fifteen lines in any app; a rule that makes
over-collection *impossible* is a framework feature, and it is the one the origin application got
right — its `record_audit` raises rather than filters, so an over-collecting call fails in
development instead of quietly accumulating in production.

## 3. Current state

**There is no audit port.** Verified: `audit` appears in `src/` only in prose — a comment in the
operation plan noting that a `before` denial happens before the handler "so audit and … " , the
agent-tools docstring saying "interceptors and audit all apply", and DST's own witness machinery.
Nothing records anything.

**The hooks that an audit needs already exist.** `Before`, `OnSuccess`, `OnFailure` and `Finally`
are the operation plan's vocabulary, and `Finally`'s contract is the load-bearing one: it *always*
runs once the scope is entered, **including when a `before` guard raises**. So a denial — the
event an audit trail most wants — is observable without touching the plan. `OnFailure`
deliberately does not run for guard denials, which is exactly why `Finally` is the hook this
design uses.

**Scrubbing exists and solves a different problem.** `forze.base.scrubbing` has `sanitize`,
`is_sensitive_key` and pattern registration; the logging processors apply it to events and to the
message. That is a **denylist** — it removes what looks sensitive. An audit trail needs the
inverse: nothing is recorded unless it was declared. A denylist that misses one key name is a
disclosure; an allowlist that misses one key is a missing field in a report.

**The invocation context already carries the actor.** `ctx.inv_ctx` is where tenancy, the
idempotency key and the execution id come from, so the actor does not need a new channel.

## 4. Goals / Non-goals

**Goals**

- Declared actions, declared metadata keys, and a refusal at the call site for anything else.
- The actor bound from the invocation context, never from the caller's payload.
- Denials audited, with the outcome, using the hook that already sees them.
- One audit row per declared effect, asserted under simulation.
- The trail is a document spec: queryable, tenant-scoped, exportable, encryptable.

**Non-goals**

- **Not a log pipeline.** The port writes rows; shipping them to a SIEM is the deployment's.
- **Not tamper-evidence.** Hash chaining and WORM storage are named in §8 and not built; claiming
  them without the storage guarantees would be false.
- **Not automatic coverage.** Nothing is audited unless declared. A framework that audited
  everything would produce the over-collection this RFC exists to prevent.
- **Not a replacement for lineage.** Who corrected a fact is
  [RFC 0052](0052-versioned-facts-correction-lineage.md)'s `Correction`; this is cross-aggregate
  who-did-what.

## 5. Design

### 5.1 The port and the record

```python
class AuditPort(Protocol):
    async def record(self, entry: AuditEntry) -> None: ...

AuditEntry(actor_ref, action, outcome, object_ref, subject_ref, metadata, at)
```

`object_ref` is what was acted on; `subject_ref` is whose data it was — the two differ whenever
one principal acts on another's records, which is the case an audit exists for. `outcome` is
`allowed | denied | failed`.

### 5.2 The spec

```python
AuditSpec(
    action="snapshot.read",
    allowed_metadata=frozenset({"snapshot_id", "purpose"}),
    audit_reads="after_authz",     # or "never"
)
```

`audit_reads` carries the origin's rule: a read is recorded **after** authorization succeeds, and
a principal reading their own data is not recorded at all. Auditing a refused read accumulates a
log of who wanted to see whose data, which is a second disclosure surface
([RFC 0055](0055-scoped-disclosure.md) decision 5) — so for reads, denials are *not* audited,
while for writes they are. Two rules, and the spec says which applies.

### 5.3 The allowlist refuses, and is checked twice

At **wiring**: every key in `allowed_metadata` must be a scalar-typed name, and a key that
`is_sensitive_key` already flags is refused outright — an allowlist naming `password` is a
declaration nobody meant to write.

At **call time**: a metadata key outside the set raises `configuration`. Not filtered — raised. A
filter is invisible, so the over-collecting call site survives to be copied; a refusal fails the
test that introduced it.

### 5.4 The decorator

```python
@audited("interval.correct", metadata=lambda ctx, args, result: {"root_id": result.root_id})
```

Wired as a `Finally` hook so the outcome is known and a guard denial is still observed. The actor
is read from `ctx.inv_ctx`; a metadata callable that raises is itself an audit failure (§5.5), not
a silent skip.

### 5.5 When the audit write fails

Default: **the operation fails.** An audited action whose audit did not land is an action nobody
can account for, and the declaration is what says it must be accountable. The escape hatch is per
spec (`on_failure="ignore"`), for actions where availability outranks the record — and
[RFC 0055](0055-scoped-disclosure.md) keeps the default for disclosure reads specifically.

This is the decision most likely to be contested, so both directions are stated: failing closed
turns an audit-store outage into an outage; failing open turns it into a silent gap. The
declaration is per action because the right answer differs per action.

### 5.6 The adapter

One `DocumentSpec` with `history_enabled=False` and a read model over
`(actor, action, outcome, object_type, object_id, subject_id, metadata, at)`. Tenant-scoped like
any document; field encryption available for `metadata`; exportable through the portability plane.
The mock's implementation is what DST reads for `audit_row_per_effect`.

### Alternatives considered

- **Scrub instead of allowlist.** The shipped `sanitize` would filter sensitive-looking keys. §3's
  argument: a denylist's miss is a disclosure, an allowlist's miss is a missing column.
- **An event on the outbox instead of a port.** Reuses shipped machinery and makes the trail
  unqueryable — "every disclosure of this record, with its reason" is the question an audit is
  read for.
- **Audit inside the adapters** (every port call recorded). Complete, enormous, and it records
  mechanism rather than intent: "selected 40 rows" is not "read Anna's March".
- **`OnSuccess` + `OnFailure` instead of `Finally`.** Misses guard denials entirely, which are the
  events §2 cares most about.

## 6. Tests

- Allowlist: a declared key records; an undeclared key raises `configuration`; a declaration
  naming a sensitive key fails at wiring; a non-scalar type fails at wiring.
- Outcome coverage: success records `allowed`; a handler failure records `failed`; an authz guard
  denial records `denied` — the last one is the `Finally` claim, and it is the battery that proves
  the hook choice.
- Actor binding: an entry's actor comes from `ctx.inv_ctx` even when the payload carries a
  different one (the payload's value is ignored, asserted).
- Read rules: `after_authz` records an admitted read and nothing for a refused one; own-data reads
  record nothing; `never` records nothing at all.
- Failure policy: with the default, a failing audit port fails the operation and the business write
  does not commit; with `ignore`, the write commits and a warning is logged.
- DST: `audit_row_per_effect` — for a workload of declared actions, exactly one row per committed
  effect, no duplicates under retry (the retry leg matters: an operation retried after a partial
  failure must not double-record).
- **Not tested:** that the trail is tamper-evident. It is not (§8).

## 7. Docs

A page section under governance: the port, the spec, the allowlist rule with its rationale in one
sentence (**an audit log built from request bodies is a second copy of the data it protects**), the
two read rules, and the failure policy with both directions stated. Plus the explicit
non-claim: no tamper-evidence.

## 8. Out of scope

- **Tamper-evidence** (hash chaining, append-only storage, external anchoring). Named because a
  DPO will ask; it needs storage guarantees the framework cannot make on an arbitrary backend.
- **Retention and purge.** A regulated trail has a retention period; expressing it needs a
  lifecycle policy, and the portability plane's scope rules already interact.
- **Read access control on the trail itself.** It is a document spec, so the app's own authz
  applies; a framework-owned rule would be wrong for most deployments.
- **Automatic per-port auditing.** §5's rejected alternative.

## 9. Risks

- **Fail-closed by default turns an audit outage into a service outage.** Deliberate, per-action
  overridable, and the single most likely row to be departed from at execution. Stated as
  `ASSUMED` for that reason.
- **The allowlist invites a lazy declaration.** An app can declare every key it wants and
  over-collect anyway. Mitigation: the wiring check refuses sensitive-looking and non-scalar keys,
  and the docs say the list is reviewed, not generated.
- **A per-action audit row on a hot path is a write amplification.** Mitigation: nothing is audited
  unless declared, so the cost is opt-in per action.
- **Read as compliance.** Shipping "audit" invites the reading that an app using it is auditable.
  Mitigation: §7's explicit non-claim.

## 10. Unresolved questions

- **Is the default really fail-closed?** The alternative (fail open, warn loudly) is defensible and
  would make adoption cheaper. Settled by the first deployment that has an opinion, and the
  decision row is graded to allow the departure.
- **Does `metadata` support nested values at all, or scalars only?** Scalars keep the allowlist
  checkable; nesting is what apps will want for an object diff.
- **Does the decorator or the port own idempotency of the row?** A retried operation must not
  double-record; the invocation's execution id is available as a natural dedup key, which makes it
  the port's business.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Metadata is an **allowlist that raises**, not a denylist that filters. A filter leaves the over-collecting call site alive to be copied; a refusal fails the test that introduced it. The shipped scrubbing is a denylist and is the wrong tool here. |
| 2 | `LOCKED` | The actor is bound from `ctx.inv_ctx` and a payload-supplied actor is ignored. An audit trail whose actor the caller can set records nothing. |
| 3 | `LOCKED` | Nothing is audited unless declared. Automatic coverage produces exactly the over-collection §2 is about. |
| 4 | `LOCKED` | The decorator wires as a `Finally` hook, because that is the only hook that runs when a `before` guard denies — and a denial is the event the trail most needs. |
| 5 | `ASSUMED` | A failed audit write **fails the operation** by default, overridable per spec. Contested on purpose: failing closed turns a store outage into an outage, failing open turns it into a silent gap, and the right answer differs per action. |
| 6 | `ASSUMED` | Reads are recorded after authorization and never for a principal's own data; refused *reads* are not recorded, while refused *writes* are. A log of refused reads is a record of who wanted to see whose data. |
| 7 | `ASSUMED` | The adapter is a `DocumentSpec`, so the trail inherits tenancy, encryption, querying and export rather than growing its own plane. |
| 8 | `OPEN` | Whether metadata values may nest, and whether row-level dedup on retry belongs to the port (using the execution id) or the decorator. |

## 12. Phasing

- **P1** — the contract package, the `Finally`-based decorator, the allowlist with both checks, the
  mock and Postgres adapters, batteries including the guard-denial leg.
- **P2** — the DST `audit_row_per_effect` invariant with its retry leg, and the kit wiring arm.
- **P3** — audit-on-read for [RFC 0055](0055-scoped-disclosure.md), once that kit exists.
- **P4** *(demand-gated)* — retention policy, nested metadata, tamper-evidence.
