# Contract-misuse operators

The reviewed taxonomy: each operator is a realistic misuse of a Forze contract by an application
author, mapped to the oracle expected to catch it and to the documented production bug class it
instantiates. The corpus (`tests/support/misuse/`) hand-authors instances as broken twins next to
their correct controls; ground truth is by construction. **The instrument is not the contribution
— the corpus exists to be measured against** (strategy statistics, mock-fidelity transfer).

Status: `P1` = instance shipped in this slice; `P2+` = planned, operator locked.

## T — concurrency & transactions

| id | operator | misuse | expected oracle | bug class / source | status |
|---|---|---|---|---|---|
| T1 | `drop_rev_guard` | replace the rev-guarded update with a blind write | conservation / at-most-once effect | lost update (Berenson P4; Jepsen analyses passim) | **P1** (`T1-blind-write-payment`) |
| T2 | `effect_before_guard` | fire a non-transactional external effect before the guarded write | `no_duplicate_effect` | premature side effect; double-charge postmortems | **P1** (`T2-charge-before-guard`) |
| T3 | `write_outside_tx` | hoist a write out of the transaction boundary | conservation over rows | partial-write torn state; dual-write family | **P1** (`T3-payment-outside-tx`); **P2 deep instance** (`T3-torn-activation`, d=2 — the torn window needs an overtake, not mere overlap) |
| T4 | `weaken_isolation` | declare a weaker `IsolationLevel` than the logic needs | `serializable` / write-skew oracle | write skew at SI (Fekete et al. 2004) | P2+ |
| T5 | `check_then_act` | unguarded read-check-write over an aggregate | cardinality invariant | TOCTOU / phantom check (Hermitage) | **P1** (`T5-unchecked-reservation`) |

## I — idempotency & retries

| id | operator | misuse | expected oracle | bug class / source | status |
|---|---|---|---|---|---|
| I1 | `drop_idempotency_key` | process a retried command without its key | at-most-once effect per command | duplicate charge on retry (Stripe/adyen-class postmortems) | **P1** (`I1-retry-without-key`) |
| I2 | `retry_without_idempotency` | wrap a non-idempotent effect in a naive retry loop | same | same, self-inflicted | P2+ |
| I3 | `ack_before_processing` | ack the delivery before the handler runs | acked ⇒ effect exists, under crash | at-most-once where at-least-once required | **P2** (`I3-ack-before-processing`, crash-restart engine) |

## M — messaging

| id | operator | misuse | expected oracle | bug class / source | status |
|---|---|---|---|---|---|
| M1 | `outbox_outside_tx` | publish to the outbox outside the state transaction | state ⇒ event exists, under crash | dual write — the canonical event-driven production bug | **P2** (`M1-dual-write-shipment`, crash-restart engine) |
| M2 | `drop_inbox_dedup` | apply the effect on every delivery, no inbox table | at-most-once effect per message | duplicate consumption (at-least-once brokers) | **P1** (`M2-consumer-without-inbox`) |
| M3 | `missing_compensation` | delete a saga compensation step | saga end-state invariant | stuck/inconsistent saga | P2+ |
| M4 | `nonidempotent_compensation` | compensation double-applies on replay | same | same | P2+ |

## D — distributed primitives

| id | operator | misuse | expected oracle | bug class / source | status |
|---|---|---|---|---|---|
| D1 | `skip_lock` | bypass the distributed lock | ledger conservation under the lease | split-brain critical section | **P2** (`D1-skip-lock`, lease-row lock) |
| D2 | `early_lock_release` | release inside the critical section | same | same | P2+ |
| D3 | `nonatomic_acquire` | check-then-set acquisition | same | same | **P2** (`D3-nonatomic-acquire`) |
| D4 | `ignore_remote_hlc` | drop the remote timestamp on HLC merge | HLC monotonicity (flagship) | causality violation | P2+ |
| D5 | `nonmonotonic_clock` | wall clock where ordering matters | same | same | P2+ |

## N — data & multitenancy

| id | operator | misuse | expected oracle | bug class / source | status |
|---|---|---|---|---|---|
| N1 | `drop_tenant_predicate` | remove the tenant filter from a query | viewer sees only own rows | cross-tenant leak | **P2** (`N1-drop-tenant-predicate`) |
| N2 | `stale_cache` | read-through cache, write-path invalidation removed | writer's read-your-writes through the cache | stale read after write | **P2** (`N2-stale-cache`) |
| N3 | `cursor_unbound_tenant` | cursor token not bound to tenant context | tenancy invariant on paged reads | cross-tenant page walk | P2+ |

## Notes

- I1 and M2 are mechanically similar in-simulation (a duplicated delivery); they are distinct
  operators because their production shapes and *correct twins* differ — an id-derived idempotent
  write vs the two-table inbox pattern (inbox insert + effect in one transaction).
- External-system reproduction (ZooKeeper/Cassandra corpora) is out of applicability by design:
  the corpus reproduces documented defect *classes* over Forze ports, and says exactly that.
- Where Forze fails closed on a misuse at wiring/call time, the corpus documents the refusal as a
  passing control instead of a mutant — that too is evidence.
