# RFC 0046 — Durable-store tenant resolution

- **Status:** 📝 Draft (execution-ready — one PR)
- **Scope:** The durable-execution plane resolves a tenant twice per operation and
  gets two different answers. A caller-supplied `tenant_id` decides the row's tag
  and its scoped id while the *relation* it lands in is resolved from the binding,
  so on a namespace deployment a record can be written where nothing will look for
  it; and the run store enforces the tenant on its control surface (`claim_abandoned`,
  `request_cancel`, `refuse_cancel`, `list_runs`) but not on its worker surface
  (`begin`, `renew`, `load`, and every terminal write), so a bound tenant that knows
  a run id reaches another tenant's run. Both hold identically in
  `forze_postgres`, `forze_mongo` and `forze_mock`, so this is a port-level change
  with three implementations and a shared battery, not a fix to one engine. Adds no
  port signature, no column and no config field; the durable configs join their
  module's existing tenancy-wiring validation. Does not change what recovery claims,
  and does not touch the step journal, whose key already carries its tenant.
- **Related:** `src/forze/application/contracts/durable/function/run_store.py`,
  `.../schedule_store.py`, the three run stores and three schedule stores under
  `forze_postgres`, `forze_mongo`, `forze_mock`,
  `src/forze/application/contracts/tenancy/wiring.py` (`TenancyRouteGroup`,
  `validate_module_tenancy`), `src/forze_kits/integrations/durable/{runner,scheduler,lifecycle}.py`
  (the shipped callers), `tests/support/durable_conformance.py`,
  `tests/integration/test_forze_postgres/test_pg_durable_tenancy_integration.py`
  (`TestTaggedTierRunControlIsolation`, the class this extends). RFC 0045 is the
  sibling shape: one missing fence, found in review, closed across every engine at once.
- **Origin:** PR #404 review. Four reviewers, two at their top severity, flagged the
  Mongo store's unscoped `load`/`_finish`; probing the plane showed Postgres and the
  mock oracle answer the same way, which made it a port question rather than a
  Mongo defect. Answered on the PR as out of scope, with this RFC as the follow-up.

---

## 1. Summary

Every durable-store operation resolves **one** effective tenant and uses it for all
three things it decides: which relation the statement runs against, what the row is
tagged with, and how a scoped id is composed. A caller-supplied tenant that
contradicts a bound one is refused rather than silently preferred for two of the
three. The run store's worker surface gains the tenant predicate its control surface
already has, written so it can never strand a run. `complete` seals the output under
the run's stored tenant instead of the binding, so a completed run is always
readable. The durable configs join their module's tenancy-wiring validation, so a
declared isolation floor covers them.

## 2. Motivation

Two defects, one root: the plane asks "which tenant?" more than once per operation
and has no rule saying the answers must agree.

**A record can be written where nothing will look for it.** `put` on the schedule
store computes `tenant_id = record.tenant_id or bound` and uses it for the stored tag
and for the scoped `_id` / `schedule_id`, while `_table()` / `_collection()` resolve
from the binding alone. On a namespace deployment (a per-tenant relation resolver)
`DurableScheduler.put(ctx, …, tenant_id=X)` called unbound writes tenant X's schedule
into the unbound relation under the id `X:sid`. X's scheduler looks in X's relation
and finds nothing; the unbound scanner reads a row whose tag it filters out. The
schedule never fires and nothing reports it. `enqueue(tenant_id=X)` on the run store
has the same shape.

**The run store enforces the boundary on four verbs and ignores it on five.**
`claim_abandoned`, `request_cancel`, `refuse_cancel` and `list_runs` all carry the
tenant predicate — the last two deliberately, with comments explaining that losing
the `status` guard makes them the widest writes on the port. `begin`, `renew`,
`load` and `_finish` (behind `complete`, `fail`, `mark_forward_incomplete`,
`mark_cancelled`, `mark_timed_out`) carry none. On a shared tagged relation a store
bound to tenant A can therefore read tenant B's run, claim it, hold its lease, and
land it in any terminal state, given the run id. `TestTaggedTierRunControlIsolation`
already asserts this boundary for the two control verbs; the worker verbs were never
included, and no test says they are exempt.

The exposure is narrow — a run id is a UUIDv7, and sealed payloads are bound to the
stored tenant's key, so an unauthorized `load` of an encrypted run returns something
it cannot open. It is still a boundary the framework declares and does not enforce,
on the plane whose stores are reached from ordinary handler code.

**Neither is reachable from a shipped caller today**, which is why this is a
follow-up rather than a hotfix. `DurableFunctionRunner._execute` binds
`record.tenant_id` for the whole execution, so the binding *is* the run's tenant on
every terminal write it makes; `DurableSchedulerLoop` binds each tenant before
`ensure_cron_schedules`, and that path passes no explicit `tenant_id`. The reachable
one is `DurableScheduler.put(ctx, …, tenant_id=X)`, a public kit method, called
without a binding.

## 3. Current state

Verified against the code, not from memory.

**The two resolutions.** `TenancyMixin._tenant_id_for_resolve()`
(`contracts/tenancy/mixins.py:43`) is the single canonical read: it returns the bound
tenant, or raises `authentication` / `tenant_required` when `tenant_aware` and
nothing is bound. Every store calls it directly *and* again inside its
`_table()` / `_collection()` helper. Within one operation the two calls always agree
— the provider is `ctx.inv_ctx.get_tenant`, reading a `ContextVar`, and
`bind_identity` is entered synchronously by the owning task, so no other task can
change it mid-operation. The disagreement is never between two reads of the binding;
it is between the binding and an explicitly passed `tenant_id`.

**Per-verb tenant handling, identical in all three run stores:**

| Verb | Relation resolved from | Tenant predicate |
| --- | --- | --- |
| `enqueue` | binding | tag written from `tenant_id or bound` |
| `begin` | binding | none |
| `renew` | binding | none |
| `claim_abandoned` | binding | `tenant_id = bound` when bound |
| `complete` / `fail` / `mark_*` (`_finish`) | binding | none |
| `refuse_cancel` | binding | `tenant_id = bound` when bound |
| `request_cancel` | binding | `tenant_id = bound` when bound |
| `load` | binding | none |
| `list_runs` | binding | `tenant_id = bound` when bound |

The schedule stores match: `claim_due` filters, `put` / `advance` / `load` / `delete`
scope the id from the binding, and `put` alone computes an effective tenant that the
relation resolution does not see.

**`complete` seals under the binding.** Both encrypting stores call
`_seal(output_json, run_id, "output", self._tenant_id_for_resolve())`, while
`_record_from_row` unseals under the row's stored `tenant_id`. The two agree because
the runner binds the run's tenant; they are not made to agree by anything the store
does.

**The step journal is not affected.** `MongoDurableFunctionStepAdapter._doc_id` and
the Postgres journal's primary key both carry the tenant, so a step row cannot be
read or written across the boundary — the composed key is the predicate.

**The durable configs are outside the wiring floor.** Both `PostgresDepsModule` and
`MongoDepsModule` build `TenancyRouteGroup`s for `document`, `search`, `outbox`,
`inbox`, `idempotency`, `counter` and friends, and neither includes `durable_step`,
`durable_run` or `durable_schedule`. `required_tenant_isolation` therefore reports a
tier derived from the other planes and says nothing about durable. The configs
already inherit `TenantAwareIntegrationConfig`, so they carry `tenant_aware`, and
their `relation` / `collection` accepts a callable — the two signals
`validate_module_tenancy` reads. No config field needs adding.

**`tenant_aware=True` on `durable_run` is currently unusable with cross-tenant
recovery**: `_tenant_id_for_resolve()` fails closed when unbound, and unbound is
exactly how `DurableRecoveryLoop` sweeps a tagged relation (`tenants=None`). This is
a real constraint the wiring floor has to respect rather than a bug to fix here.

## 4. Goals / Non-goals

**Goals**

- One effective tenant per operation, used for the relation, the tag and the scoped id.
- A contradiction between an explicit tenant and a bound one is refused, not resolved.
- The run store's worker surface enforces the same boundary as its control surface.
- A completed run is always readable: the output's AAD tenant matches the row's.
- A declared `required_tenant_isolation` covers the durable routes.
- All three implementations move together, pinned by the shared battery.

**Non-goals**

- Changing what `claim_abandoned` claims. Unbound recovery over a tagged relation
  keeps sweeping every tenant; that is the documented model and the runner re-binds
  each run's tenant to execute it.
- Making `tenant_aware=True` work with unbound recovery. The fail-closed read is
  correct; a deployment that wants tagged-tier enforcement on the run store runs
  per-tenant recovery. §5.4 declares the constraint rather than removing it.
- Touching the step journal. Its key carries the tenant already.
- A new isolation tier, a new config field, or a port signature change.
- Making a refused write distinguishable from a no-op write. `_finish` is already
  silently idempotent by design; see §9.

## 5. Design

### 5.1 One effective tenant, and a refused contradiction

Both stores that accept an explicit tenant gain the same two-line rule, on the
contract side of the mixin so all three engines share one implementation:

```python
# forze/application/contracts/tenancy/mixins.py, on TenancyMixin
def _effective_tenant(self, requested: UUID | None) -> UUID | None:
    """The one tenant an operation resolves against: the caller's, or the binding.

    Refuses a *contradiction* — an explicit tenant that differs from a bound one —
    rather than letting the two decide different halves of the same write.
    """

    bound = self._tenant_id_for_resolve()

    if requested is None:
        return bound

    if bound is not None and requested != bound:
        raise exc.authentication(
            "Requested tenant does not match the bound tenant.",
            code="tenant_mismatch",
        )

    return requested
```

The three cases, in full:

| bound | requested | result |
| --- | --- | --- |
| — | — | unbound; unchanged |
| A | — | A; unchanged |
| A | A | A; unchanged |
| A | B | **refused** — `tenant_mismatch` |
| — | B | **B**, and the relation now resolves under B |

Only the last two rows change behaviour, and the last is the one that fixes the
misplaced record: `enqueue(tenant_id=B)` and `put(record.tenant_id=B)` unbound now
resolve their relation under B, so the row lands where B's reader looks.

Callers thread the resolved value through instead of re-reading:

```python
async def enqueue(self, name, *, input_json, idempotency_key=None,
                  tenant_id=None, available_at=None):
    tenant_id = self._effective_tenant(tenant_id)
    table = await self._table(tenant_id)          # was: await self._table()
    ...
```

`_table()` / `_collection()` take an optional tenant and keep their current
behaviour when it is omitted, so the verbs that have no explicit tenant are untouched.

`exc.authentication` with a distinct `code` matches how the mixin already refuses a
missing tenant (`tenant_required`); a mismatch is the same class of failure — the
caller's identity does not permit the write it asked for.

### 5.2 The worker surface

`begin`, `renew`, `load` and `_finish` gain the predicate the control verbs carry,
in one shape that cannot strand a run:

```
(tenant_id = <bound>  OR  tenant_id IS NULL)     -- when a tenant is bound
(no predicate)                                   -- when none is
```

The `IS NULL` arm is load-bearing. A run tagged with no tenant belongs to no tenant,
and `DurableFunctionRunner._execute` leaves the ambient binding alone for such a run
(`nullcontext()` when `record.tenant_id is None`). Without the arm, a NULL-tagged run
finished while some unrelated tenant happens to be bound would match nothing: the
terminal write would no-op, the run would sit `RUNNING` until its lease expired, be
reclaimed, and re-run — forever. With it, the untagged run stays completable from
anywhere, which is what "belongs to no tenant" has to mean, and a *tagged* run is
reachable only from its own tenant or from an unbound (system) caller.

Unbound is deliberately unfiltered: it is the recovery and single-tenant role, and
narrowing it would break the sweep this RFC promises not to change.

On Mongo the same predicate is `{"$or": [{"tenant_id": str(bound)}, {"tenant_id": None}]}`,
with the standing caveat that `{field: None}` matches missing as well as null — which
is the intent here, since a row written before the tag existed is untagged.

### 5.3 Sealing under the run's own tenant

`complete` reads the run's stored tenant and seals with that:

```python
async def complete(self, run_id, *, output_json, fence=None):
    stored_tenant = await self._stored_tenant(run_id)      # one SELECT / find_one
    sealed = await self._seal(output_json, run_id, "output", stored_tenant)
    await self._finish(run_id, status=COMPLETED, output=sealed, error=None, fence=fence)
```

One extra read on a once-per-run operation, in exchange for the guarantee that the
output's AAD tenant is the tenant `_record_from_row` will unseal with. It is needed
precisely because §5.2 keeps NULL-tagged runs completable under any binding: for
those, the binding and the stored tenant genuinely differ, and sealing under the
binding would write output the store can never open.

Where the run is absent the read returns `None` and `_finish` matches nothing, so the
call stays the no-op it already is. The mock stores no ciphertext and needs no change.

### 5.4 The wiring floor

Both modules add three groups:

```python
TenancyRouteGroup(
    kind="durable_run",
    configs=_as_routes("durable_run", self.durable_run),
    tenant_aware=lambda cfg: cfg.tenant_aware,
    namespace_resolver=lambda cfg: cfg.relation,      # collection, on Mongo
),
# …the same for durable_step and durable_schedule
```

`_as_routes` is a two-line helper turning an optional single config into the
`{name: config}` mapping `TenancyRouteGroup` expects, since these are scalar fields
rather than route maps. Nothing else about `validate_module_tenancy` changes: a
dynamic (callable) relation already marks the namespace tier, and a static one does
not.

The consequence, stated so a deployment is not surprised by it: declaring
`required_tenant_isolation="tagged"` now requires `tenant_aware=True` on the durable
configs, and `tenant_aware=True` on `durable_run` is incompatible with unbound
cross-tenant recovery (§3). Such a deployment runs `DurableRecoveryLoop(tenants=…)`.
That is a real coupling, and §7 documents it rather than hiding it.

### Alternatives considered

**Prefer the explicit tenant everywhere, refuse nothing.** Simpler, and it fixes the
misplaced record — but it lets a store bound to A write into B's namespace on
purpose, which is the escalation the worker-surface predicate exists to stop. Refusing
the contradiction costs one branch and closes both.

**Refuse any explicit tenant when a binding exists.** Stricter, and it would break
the equal case (`bound == requested`), which is how `DurableSchedulerLoop` already
calls `put` — it binds the tenant *and* passes it.

**Scope the worker surface on the exact tenant, without the `IS NULL` arm.** Tighter
on paper, and it strands untagged runs in a re-run loop (§5.2). Rejected on the
failure mode, not on the strictness.

**Have the runner bind `tenant=None` explicitly for untagged runs**, making the
binding always equal the stored tenant and letting the predicate be exact. It works,
but `bind_identity` clears `authn` alongside `tenant`, so it would change what an
untagged run executes under — a larger behavioural change than the defect warrants.

**Fix Mongo only, since that is where it was reported.** Rejected on arrival: the
shared battery compares all three against one oracle, so one engine answering
differently is a conformance failure, and the other two would keep the defect.

## 6. Tests

**The shared battery gains a tenancy scenario** (`tests/support/durable_conformance.py`),
so mock, Postgres and Mongo answer it identically:

- a run enqueued under tenant A is invisible to `load` bound to B, and `begin`,
  `renew` and every terminal verb bound to B leave it untouched;
- the same run under A's binding behaves exactly as it does today;
- an untagged run is loadable and completable under any binding, and under none;
- `enqueue(tenant_id=B)` while bound to A is refused with `tenant_mismatch`, and so
  is `put` on a schedule record carrying B;
- `enqueue(tenant_id=B)` unbound lands a run B can load.

**Namespace placement gets its own leg** on the two relational engines, since the
mock has no relation to resolve: a schedule put unbound for tenant B is loadable
under B's binding — which is the assertion that fails today.

**The tagged-tier isolation class is extended** rather than duplicated:
`TestTaggedTierRunControlIsolation` in
`tests/integration/test_forze_postgres/test_pg_durable_tenancy_integration.py`
already pins the boundary for `request_cancel` and `refuse_cancel`; the worker verbs
join it, and Mongo gets the mirror.

**A sealed-output leg**: a run completed while its own tenant is bound, and an
untagged run completed under an unrelated binding, both load back with their output
readable. The second fails today.

**Not tested:** that two reads of the binding within one operation agree. It is a
property of `ContextVar`, not of this code, and a test for it would assert the
runtime rather than the design.

## 7. Docs

- `pages/docs/data-events/durable-execution.md` — the tenancy paragraph gains the
  rule (one effective tenant; a contradiction is refused) and the coupling between
  `required_tenant_isolation="tagged"` and per-tenant recovery.
- `pages/docs/reference/contracts/durable.md` — the per-verb boundary, stated once
  for the port rather than per engine.
- Both integration pages inherit it by pointing at the contract page; neither
  restates it.
- The changelog entry says what a deployment can now rely on, and names the one
  behaviour that changes for existing callers (an explicit tenant contradicting a
  binding is refused where it used to be half-honoured).

## 8. Out of scope

- **The step journal's double read** of the tenant provider. It is one redundant
  call, not a defect — the two reads cannot disagree (§3) — and collapsing it is
  tidying that would touch three adapters for no behaviour change.
- **A distinguishable refusal for a cross-tenant terminal write.** Named as the
  escape hatch, not built: `_finish` returns nothing today and a late completion is
  a legitimate no-op, so a caller cannot tell "already terminal" from "not yours"
  without a port signature change. §9 records the consequence.
- **`DurableRunAdminPort` beyond what it already does.** `list_runs` and
  `request_cancel` are scoped today and stay as they are.
- **Inngest and Temporal.** Neither implements these ports; their tenancy is the
  hosted engine's.

## 9. Risks

- **A deployment that relied on the half-honoured explicit tenant breaks loudly.**
  A call passing a tenant that contradicts its binding now raises where it used to
  write a misplaced row. Loud is the point, and no shipped caller does it — but it is
  a behaviour change, and the changelog says so plainly.
- **The `IS NULL` arm reads as a hole.** It permits any binding to finish an untagged
  run. That is deliberate (§5.2) and the alternative strands runs; the code says why
  at the predicate, not only here, because this is exactly the line a future reader
  will try to tighten.
- **A cross-tenant terminal write is refused silently.** It joins the existing set of
  writes that match nothing, so an attacker learns nothing and a confused caller
  learns nothing either. Accepted: making it distinguishable is a port change (§8).
- **The wiring floor newly rejects configurations that used to pass.** Only where a
  floor is declared *and* the durable configs do not meet it — which is the point of
  declaring one, but it will surface at startup for someone. The error names the
  route and the tier, as the other planes' do.
- **The extra read in `complete`** doubles that call's round trips. It is once per
  run, against the row about to be updated, and therefore in cache.

## 10. Unresolved questions

- Whether `_effective_tenant` belongs on `TenancyMixin` (shared by every adapter that
  takes an explicit tenant, of which the durable stores are today the only ones) or in
  the durable contracts package. Locked to the mixin below on the argument that the
  rule is about tenancy rather than about durability; if a second plane never wants
  it, the cost of the choice is one method on a widely-inherited mixin.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | One effective tenant per operation, used for the relation, the tag and the scoped id. This is the whole RFC; every other row is how it is applied. |
| 2 | `LOCKED` | An explicit tenant contradicting a bound one is **refused** (`authentication` / `tenant_mismatch`), not preferred and not ignored. Preferring it would sanction a cross-tenant write; ignoring it is today's defect. Consequence: a caller that passes both must pass them equal. |
| 3 | `LOCKED` | The worker-surface predicate is `tenant_id = bound OR tenant_id IS NULL`, never the exact match. The exact match strands untagged runs in a reclaim loop; this is a failure-mode decision, not a strictness preference, and tightening it later re-opens that loop. |
| 4 | `LOCKED` | Unbound stays unfiltered. It is the recovery and single-tenant role, and narrowing it would change what `claim_abandoned` claims — explicitly a non-goal. |
| 5 | `ASSUMED` | `complete` reads the run's stored tenant before sealing. One extra read on a terminal operation buys an always-readable output; if the read proves measurably costly, the alternative is to seal under the binding and refuse untagged runs a mismatched completion — which trades a hang for a cost. |
| 6 | `ASSUMED` | `_effective_tenant` lives on `TenancyMixin` rather than in the durable contracts (see §10). |
| 7 | `ASSUMED` | The durable configs join the existing `TenancyRouteGroup` machinery unchanged, via a small helper that lifts a scalar config into a one-entry route map. No new tier, no new field. |
| 8 | `LOCKED` | All three implementations move in one change, pinned by the shared battery. A per-engine rollout would put the oracle in disagreement with an engine at every intermediate commit. |
| 9 | `OPEN` | Whether the untagged-run allowance also applies to `request_cancel` / `refuse_cancel` / `list_runs`, which today match on the exact tenant and therefore cannot see an untagged run from a bound caller. Execution decides: making them consistent with §5.2 is defensible, and so is leaving the control surface strict. Whichever, the battery states it. |
| 10 | `OPEN` | Whether the step journal's configs get a `TenancyRouteGroup` too. Its key already carries the tenant, so the group adds no enforcement — but omitting it leaves one durable route invisible to a declared floor. Execution decides and logs it. |

## 12. Phasing

One PR. The pieces are not independently shippable: §5.1 and §5.2 change what the
shared battery asserts, and §5.4 is what makes a declared floor tell the truth about
the routes the first two harden. Order within the branch: contract helper → the three
run stores → the three schedule stores → the battery → the wiring groups → docs.
