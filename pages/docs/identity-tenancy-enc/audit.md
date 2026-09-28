---
title: Audit trail
icon: lucide/scroll-text
summary: Declared actions, an allowlist of metadata that refuses, and one row per audited operation — denials included
---

An audit trail answers "who did what, to whose data, and was it allowed". The way one goes wrong
is rarely by recording too little: **an audit log built from request bodies is a second copy of
the data it protects** — unencrypted, long-retained, readable by whoever reads the logs. So the
point of the audit plane is the allowlist: an action records only the metadata keys it declared,
and a call that tries to record anything else **raises** instead of being filtered. A filter is
invisible and the over-collecting call site survives to be copied; a refusal fails the test that
introduced it.

Nothing is audited unless declared.

## Declaring an action

```python
from forze.application.contracts.audit import AuditObjectRef, AuditSpec
from forze.application.hooks.audit import Audited

INTERVAL_CORRECT = AuditSpec(
    action="interval.correct",
    allowed_metadata=frozenset({"root_id", "reason_code"}),
)

audited = Audited(
    spec=INTERVAL_CORRECT,
    metadata=lambda args, result: {"root_id": args.root_id, "reason_code": args.reason_code},
    object_ref=lambda args, result: AuditObjectRef(type="interval", id=str(args.root_id)),
)
```

Then bind it to the operations it covers, alongside their guards and transaction:

```python
registry = audited.bind(
    registry.bind("interval.correct").bind_tx().set_route("pg").finish()
).finish()
```

- **Metadata is scalars** — strings, integers, finite floats, booleans, `None`, and UUIDs (stored as
  strings). A key outside `allowed_metadata`, or a value that is not a scalar, raises
  `configuration` (code `audit_metadata_refused`), naming the key and never the value.
- **A name the log scrubber treats as a secret** (`password`, `token`, `api_key`, `session`…) is
  refused when the spec is declared: an allowlist naming one is a declaration nobody meant.
- **The actor and subject come from the authenticated identity**, never from the arguments.
  `subject_id` is the principal the call runs for; `actor_id` is who performed it — the same
  principal, or the delegate when the call is delegated.

## What gets recorded, and where

Every audited operation ends in one row, with an outcome of `allowed`, `denied` or `failed`.
Where the row is written depends on the outcome, because that decides whether it can be trusted:

| The operation… | Outcome | The row is written… |
|----------------|---------|---------------------|
| completes | `allowed` | inside its transaction — it commits with the operation's own write, or rolls back with it |
| is refused by a guard (authentication or authorization) | `denied` | after it returns, in a write of its own |
| is admitted and then fails | `failed` | after its transaction rolled back, in a write of its own |

A denial is the event a trail most needs, and it happens before the handler and before the
transaction opens — which is why the audit hooks are bound to the operation's outer scope as well
as its transaction. An operation without a transaction binds with `transactional=False`, and its
`allowed` row is written after it returns.

**Reads** (`QUERY` operations) follow two rules of their own, set by `audit_reads`:

- `"after_authz"` (the default) records a read once it is admitted and completes. A refused or
  failed read is **not** recorded: a log of refused reads is a record of who wanted to see whose
  data, a second disclosure surface. The row is written after the read, since the read's own
  transaction is read-only.
- A principal reading their **own** data, undelegated, is not recorded. Pass `owner=` — a callable
  naming whose data the read returned — for the audit to tell; without it, every admitted read is
  recorded, since the skip is never a guess.
- `"never"` records no reads for that action.

## When the audit write fails

`on_failure="fail"` (the default): an operation whose audit row could not be written **fails**, and
its write rolls back — an audited action nobody can account for did not happen.
`on_failure="ignore"`: the operation succeeds and a warning is logged. Neither direction is free —
failing closed turns an audit-store outage into an outage, failing open turns it into a silent
gap — so the choice is per action.

A metadata refusal raises under either policy: it is the over-collecting call site, not an
outage. And a `failed` or `denied` operation keeps its own error — a row that cannot be written
for it is logged, never raised in its place.

## The collection

The trail is an ordinary document collection, so it is tenant-scoped, queryable, exportable and
encryptable like any other. `forze_kits.integrations.audit` provides the spec and the port:

```python
from forze_kits.integrations.audit import AuditDepsModule, audit_record_spec

AUDIT = audit_record_spec()           # route "audit_events"; wire it like any document spec
audit_deps = AuditDepsModule(tx_route="pg")
```

Route `AUDIT` through your document deps module on the database the audited operations' own
transactions use — an `allowed` row is written inside them — and register it in your spec
inventory. The collection is yours to create:

```sql
CREATE TABLE audit_events (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,          -- when wired tenant-aware
    action text NOT NULL,
    outcome text NOT NULL,
    actor_id uuid,
    subject_id uuid,
    object_type text,
    object_id text,
    metadata jsonb NOT NULL DEFAULT '{}',
    at timestamptz NOT NULL
);
```

To keep the trail somewhere else, implement `AuditPort` — one `record(entry)` that joins the
open transaction when there is one — and register its factory under `AuditDepKey`, as
`AuditDepsModule` does.

`audit_record_spec(encryption=...)` may seal `metadata`; the other fields are what the trail is
queried by, and sealing one is refused.

!!! warning "Not tamper-evident"

    The trail is rows in your database, and whoever can write that table can rewrite it. Hash
    chaining, append-only storage and external anchoring need guarantees the framework cannot
    make on an arbitrary backend, and none of them is built. Using the audit plane does not make
    an application auditable on its own; it records what was declared.
