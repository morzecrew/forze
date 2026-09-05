"""The durable-run scenarios every engine behind the port must answer identically.

Three scenario functions, each driving one store through a lifecycle and returning the
observable outcomes as a plain dict, so a leg is `assert mock_out == engine_out` plus a few
anchors that keep a *matching pair of wrong answers* from passing. Kept here rather than in
one engine's test file because the value is the comparison: a scenario that lives beside
Postgres drifts the moment a second engine needs one, and then each backend is verified
against a different subset — the failure the shared batteries in this directory exist to
prevent.

Deliberately plane-independent. Run ids and timestamps differ per engine (each mints its
own), so what is compared is names in order, page shapes, filter counts, and the *shape* of
every control decision: which asks were accepted, which landings took effect, which the
fence refused, which stamps ended up set.

Used by:

- ``tests/integration/test_forze_postgres/test_pg_durable_conformance.py`` (mock vs Postgres)
- ``tests/integration/test_forze_mongo/test_mongo_durable_conformance.py`` (mock vs Mongo)

The forced-schedule race batteries live next door in ``durable_cancel_races``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    DurableRunAdminPort,
    DurableRunContext,
    DurableRunStatus,
    DurableRunStorePort,
    DurableScheduleRecord,
    DurableScheduleStorePort,
    bind_durable_run,
    durable_run_control_capabilities,
    reset_durable_run,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow

# ----------------------- #


async def run_lifecycle_scenario(
    store: DurableRunStorePort,
    step_of: Callable[[], DurableFunctionStepPort],
) -> dict[str, Any]:
    """Drive one durable lifecycle and collect the observable outcomes."""

    out: dict[str, Any] = {}

    first = await store.enqueue("fn", input_json={"n": 1}, idempotency_key="k")
    out["enqueue_status"] = first.status.value

    resubmit = await store.enqueue("fn", input_json={"n": 2}, idempotency_key="k")
    out["idempotent_same_run"] = first.run_id == resubmit.run_id
    out["idempotent_keeps_original_input"] = resubmit.input_json == {"n": 1}

    claimed = await store.begin(first.run_id, lease_for=timedelta(minutes=5))
    out["claimed_status"] = None if claimed is None else claimed.status.value
    out["claimed_attempts"] = None if claimed is None else claimed.attempts
    out["reclaim_while_running"] = (
        await store.begin(first.run_id, lease_for=timedelta(minutes=5)) is None
    )

    calls: list[int] = []
    token = bind_durable_run(DurableRunContext(run_id=first.run_id, name="fn"))
    try:
        step = step_of()

        async def work() -> dict:
            calls.append(1)
            return {"v": 42}

        out["step_result"] = await step.run("s1", work)
        out["step_replay"] = await step.run("s1", work)
    finally:
        reset_durable_run(token)

    out["step_ran_once"] = len(calls)

    await store.complete(first.run_id, output_json={"done": True})
    loaded = await store.load(first.run_id)
    out["final_status"] = None if loaded is None else loaded.status.value
    out["final_output"] = None if loaded is None else loaded.output_json

    abandoned = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
    out["completed_not_reclaimed"] = first.run_id not in {a.run_id for a in abandoned}

    return out


# ....................... #


async def run_list_scenario(store: DurableRunStorePort) -> dict[str, Any]:
    """Drive `list_runs` and collect plane-independent observables (names / counts).

    Run ids and timestamps differ between the two engines (each generates its own), so the
    outcomes compared are the *names* in newest-first order, page shapes, and filter counts —
    all identical across a faithful stand-in.
    """

    admin = cast(DurableRunAdminPort, store)

    records = [await store.enqueue(f"fn{i}", input_json={"i": i}) for i in range(5)]

    # Complete the middle run so the status filter has something to select.
    mid = records[2]
    await store.begin(mid.run_id, lease_for=timedelta(minutes=5))
    await store.complete(mid.run_id, output_json={"ok": True})

    out: dict[str, Any] = {}

    full = await admin.list_runs(limit=10)
    out["all_names_newest_first"] = [r.name for r in full.records]
    out["all_next_cursor_is_none"] = full.next_cursor is None
    out["created_at_populated"] = all(r.created_at is not None for r in full.records)

    page1 = await admin.list_runs(limit=2)
    out["page1_names"] = [r.name for r in page1.records]
    out["page1_has_cursor"] = page1.next_cursor is not None

    page2 = await admin.list_runs(limit=2, cursor=page1.next_cursor)
    out["page2_names"] = [r.name for r in page2.records]

    completed = await admin.list_runs(status=DurableRunStatus.COMPLETED, limit=10)
    out["completed_names"] = [r.name for r in completed.records]

    by_name = await admin.list_runs(name="fn0", limit=10)
    out["by_name_count"] = len(by_name.records)

    # A page of nothing is a caller error, not an empty page: every engine refuses it the
    # same way, so a boundary that slipped through would be one engine's private behaviour.
    try:
        await admin.list_runs(limit=0)
    except CoreException as error:
        out["zero_limit_list"] = error.kind.value
    else:  # pragma: no cover - a store that allows it is the failure being pinned
        out["zero_limit_list"] = "allowed"

    return out


# ....................... #


async def run_control_scenario(store: DurableRunStorePort) -> dict[str, Any]:
    """Drive the full run-control battery and collect plane-independent observables.

    Timestamps and run ids differ per engine, so what is compared is the *shape* of every
    decision: which asks were accepted, which landings took effect, which were refused by
    the fence, and which stamps ended up set.
    """

    admin = cast(DurableRunAdminPort, store)
    out: dict[str, Any] = {}

    # 1. A PENDING run stops at once, and the recovery scan never sees it again.
    pending = await store.enqueue("cancel-pending", input_json=None)
    out["pending_ask"] = await admin.request_cancel(pending.run_id)

    landed = await store.load(pending.run_id)
    out["pending_status"] = None if landed is None else landed.status.value
    out["pending_stamped"] = landed is not None and landed.cancel_requested_at is not None

    claimed_ids = {
        r.run_id for r in await store.claim_abandoned(limit=50, lease_for=timedelta(minutes=5))
    }
    out["pending_not_reclaimed"] = pending.run_id not in claimed_ids

    # 6. Terminal now: the ask reports that nothing happened.
    out["terminal_ask"] = await admin.request_cancel(pending.run_id)

    # 2 + 6. A RUNNING run is only stamped; asking twice is idempotent down to the instant.
    running = await store.enqueue("cancel-running", input_json=None)
    holder = await store.begin(running.run_id, lease_for=timedelta(minutes=5))
    assert holder is not None

    out["running_ask"] = await admin.request_cancel(running.run_id)
    out["running_ask_again"] = await admin.request_cancel(running.run_id)

    stamped = await store.load(running.run_id)
    out["running_still_running"] = stamped is not None and stamped.status.value == "running"
    first_ask = None if stamped is None else stamped.cancel_requested_at

    reasked = await store.load(running.run_id)
    out["ask_instant_is_stable"] = reasked is not None and reasked.cancel_requested_at == first_ask

    # The holder learns about it on the heartbeat it was making anyway.
    renewal = await store.renew(
        running.run_id, lease_for=timedelta(minutes=5), fence=holder.attempts
    )
    out["renewal_held"] = renewal.held
    out["renewal_carries_cancel"] = renewal.cancel_requested

    # 4. A stale fence can neither renew nor land the cancel.
    stale = await store.renew(
        running.run_id, lease_for=timedelta(minutes=5), fence=holder.attempts + 99
    )
    out["stale_renewal_held"] = stale.held
    out["stale_renewal_cancel"] = stale.cancel_requested

    await store.mark_cancelled(running.run_id, fence=holder.attempts + 99)
    unmoved = await store.load(running.run_id)
    out["stale_landing_ignored"] = unmoved is not None and unmoved.status.value == "running"

    # The current holder lands it.
    await store.mark_cancelled(running.run_id, fence=holder.attempts)
    cancelled = await store.load(running.run_id)
    out["running_final_status"] = None if cancelled is None else cancelled.status.value

    # 3. A late completion against a landed cancel is a no-op, not an overwrite.
    await store.complete(running.run_id, output_json={"late": True}, fence=holder.attempts)
    after_race = await store.load(running.run_id)
    out["late_complete_status"] = None if after_race is None else after_race.status.value
    out["late_complete_output"] = None if after_race is None else after_race.output_json

    # 7. A refusal is recorded whatever the run's state, but only under a matching fence.
    refused = await store.enqueue("cancel-refused", input_json=None)
    refused_holder = await store.begin(refused.run_id, lease_for=timedelta(minutes=5))
    assert refused_holder is not None

    await admin.request_cancel(refused.run_id)
    await store.refuse_cancel(refused.run_id, fence=refused_holder.attempts + 99)
    not_refused = await store.load(refused.run_id)
    out["stale_refusal_ignored"] = not_refused is not None and not_refused.cancel_refused_at is None

    await store.refuse_cancel(refused.run_id, fence=refused_holder.attempts)
    mid_flight = await store.load(refused.run_id)
    out["refused_while_running"] = (
        mid_flight is not None and mid_flight.cancel_refused_at is not None
    )

    # An unfenced refusal is the port's other shape: a caller with no claim of its own
    # (an operator tool, a bridge) records that the ask was declined, and lands on the
    # first instant just the same.
    unfenced = await store.enqueue("cancel-refused-unfenced", input_json=None)
    assert await store.begin(unfenced.run_id, lease_for=timedelta(minutes=5)) is not None
    await admin.request_cancel(unfenced.run_id)
    await store.refuse_cancel(unfenced.run_id)
    unfenced_pending = await store.load(unfenced.run_id)
    out["unfenced_refusal_recorded"] = (
        unfenced_pending is not None and unfenced_pending.cancel_refused_at is not None
    )

    await store.complete(
        refused.run_id, output_json={"forward": True}, fence=refused_holder.attempts
    )
    completed = await store.load(refused.run_id)
    out["refused_status"] = None if completed is None else completed.status.value
    out["refused_asked"] = completed is not None and completed.cancel_requested_at is not None
    out["refused_stamped"] = completed is not None and completed.cancel_refused_at is not None

    # 7b. Refusing a run that has ALREADY landed — the ordering production actually takes,
    # and the one the "not guarded on RUNNING" promise exists for. The runner stamps the
    # refusal from its ``finally``, which runs after the terminal write, so a store that
    # quietly guarded this write like every other terminal write would drop every real
    # refusal while passing the mid-flight case above.
    late = await store.enqueue("cancel-refused-late", input_json=None)
    late_holder = await store.begin(late.run_id, lease_for=timedelta(minutes=5))
    assert late_holder is not None

    await admin.request_cancel(late.run_id)
    await store.complete(late.run_id, output_json={"forward": True}, fence=late_holder.attempts)
    await store.refuse_cancel(late.run_id, fence=late_holder.attempts)

    landed_late = await store.load(late.run_id)
    out["late_refusal_status"] = None if landed_late is None else landed_late.status.value
    out["late_refusal_stamped"] = (
        landed_late is not None and landed_late.cancel_refused_at is not None
    )

    # 8. The two terminals that are neither success nor cancellation: an ordinary failure,
    # and a saga that committed at its pivot and could not finish going forward. Each is a
    # distinct state because each sends an operator somewhere different, and a store that
    # collapsed either into ``failed`` would be readable only by reading its code.
    failing = await store.enqueue("failed-run", input_json=None)
    failing_holder = await store.begin(failing.run_id, lease_for=timedelta(minutes=5))
    assert failing_holder is not None

    await store.fail(failing.run_id, error="boom", fence=failing_holder.attempts)
    failed = await store.load(failing.run_id)
    out["failed_status"] = None if failed is None else failed.status.value
    out["failed_error"] = None if failed is None else failed.error

    # A stale worker cannot fail a run it no longer holds, like every other terminal write.
    await store.fail(failing.run_id, error="late", fence=failing_holder.attempts + 99)
    still_failed = await store.load(failing.run_id)
    out["failed_error_after_stale_write"] = None if still_failed is None else still_failed.error

    pivoted = await store.enqueue("forward-incomplete", input_json=None)
    pivot_holder = await store.begin(pivoted.run_id, lease_for=timedelta(minutes=5))
    assert pivot_holder is not None

    await store.mark_forward_incomplete(
        pivoted.run_id, error="pivot committed, step 3 failed", fence=pivot_holder.attempts
    )
    incomplete = await store.load(pivoted.run_id)
    out["forward_incomplete_status"] = None if incomplete is None else incomplete.status.value
    out["forward_incomplete_error"] = None if incomplete is None else incomplete.error

    # 9. The deadline terminal is its own state on both engines.
    expired = await store.enqueue("timed-out", input_json=None)
    expired_holder = await store.begin(expired.run_id, lease_for=timedelta(minutes=5))
    assert expired_holder is not None

    await store.mark_timed_out(expired.run_id, error="cap exceeded", fence=expired_holder.attempts)
    timed_out = await store.load(expired.run_id)
    out["timed_out_status"] = None if timed_out is None else timed_out.status.value
    out["timed_out_error"] = None if timed_out is None else timed_out.error

    # Both terminals are selectable through the ops listing, not just readable per-run.
    by_cancelled = await admin.list_runs(status=DurableRunStatus.CANCELLED, limit=10)
    by_timed_out = await admin.list_runs(status=DurableRunStatus.TIMED_OUT, limit=10)
    out["cancelled_names"] = sorted(r.name for r in by_cancelled.records)
    out["timed_out_names"] = sorted(r.name for r in by_timed_out.records)

    out["supports_cancel"] = durable_run_control_capabilities(store).supports_cancel

    return out


# ....................... #


async def run_claim_scenario(store: DurableRunStorePort) -> dict[str, Any]:
    """Drive the recovery scan's exclusivity and collect plane-independent observables.

    The rule every engine has to enforce and each one enforces differently: a run claimed by
    one scanner is **not** claimable by the next until its lease lapses. Postgres gets it
    from ``FOR UPDATE SKIP LOCKED`` plus the lease predicate; Mongo re-checks the claimable
    predicate inside the batch update, because its candidate read holds no lock. A store
    that dropped the re-check would let the second scan steal a live claim, bump the fence
    out from under the first, and leave a worker executing a run it no longer holds.

    Also pins the two filters the scan applies before anything else: a delayed run is not
    due, and a lapsed lease is reclaimable — with the *fence advancing* on the reclaim,
    which is what makes the previous holder's writes bounce.
    """

    out: dict[str, Any] = {}

    ready = await store.enqueue("claimable", input_json=None)
    delayed = await store.enqueue(
        "delayed",
        input_json=None,
        available_at=utcnow() + timedelta(hours=1),
    )

    first = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
    out["first_scan_names"] = sorted(record.name for record in first)
    out["delayed_not_claimed"] = delayed.run_id not in {record.run_id for record in first}

    # The whole point: a live lease is not a claim opportunity.
    second = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
    out["second_scan_names"] = sorted(record.name for record in second)

    # ...and the first scanner still holds what it took, at the fence it was handed.
    claimed = next(record for record in first if record.run_id == ready.run_id)
    renewal = await store.renew(
        ready.run_id, lease_for=timedelta(minutes=5), fence=claimed.attempts
    )
    out["holder_still_holds"] = renewal.held

    # A single-run claim of an already-running run is refused for the same reason.
    out["begin_while_running"] = (
        await store.begin(ready.run_id, lease_for=timedelta(minutes=5)) is None
    )

    # A scanner's budget reaches zero routinely — it must idle, not fail. Asserted here
    # because each engine expresses it differently (``LIMIT 0``, a list slice, a driver that
    # refuses a zero cursor length outright) and only one of those is free.
    out["zero_limit_scan"] = [
        record.run_id
        for record in await store.claim_abandoned(limit=0, lease_for=timedelta(minutes=5))
    ]

    return out


# ....................... #


def tenant_provider_for(tenant: UUID | None) -> Callable[[], TenantIdentity | None]:
    """A tenant provider for one binding, including the unbound one.

    Lives here rather than in each engine's leg because the scenario below is *about* two
    stores that differ only by this, and two copies of it would be two chances to bind them
    differently.
    """

    return lambda: None if tenant is None else TenantIdentity(tenant_id=tenant)


# ....................... #


async def run_tenancy_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
    schedules_for: Callable[[UUID | None], DurableScheduleStorePort],
) -> dict[str, Any]:
    """Drive the tenant boundary from both sides of it.

    Takes factories rather than a store, because every observable here is about two stores
    that differ *only* by their binding, over one relation. The engine's leg supplies stores
    sharing one relation; the mock's supplies stores sharing one ``MockState``.

    Three properties, and the reason each is not the obvious one:

    - The verbs taking a run id reach the bound tenant's runs and untagged ones, and nothing
      else. The untagged arm is liveness, not laxity: a run belonging to no tenant whose
      terminal write matched nothing would be reclaimed and re-run forever.
    - The verbs that enumerate match the tenant exactly, so an untagged run is completable
      under a binding and invisible to that binding's listing. The asymmetry is deliberate.
    - An explicit tenant is used for the relation, the tag and the scoped id together, or
      refused. Contradicting a binding refuses; naming one where nothing is bound is
      honoured, which is what puts the row where its owner will look for it.
    """

    out: dict[str, Any] = {}
    tenant_a = UUID("00000000-0000-0000-0000-00000000000a")
    tenant_b = UUID("00000000-0000-0000-0000-00000000000b")

    as_a = store_for(tenant_a)
    as_b = store_for(tenant_b)
    unbound = store_for(None)

    # 1. A's run is out of B's reach on every verb that takes a run id.
    owned = await as_a.enqueue("owned", input_json={"t": "a"})

    out["cross_load"] = await as_b.load(owned.run_id) is None
    out["cross_begin"] = await as_b.begin(owned.run_id, lease_for=timedelta(minutes=5)) is None

    held = await as_a.begin(owned.run_id, lease_for=timedelta(minutes=5))
    assert held is not None
    out["own_begin"] = held.status.value

    out["cross_renew"] = (
        await as_b.renew(owned.run_id, lease_for=timedelta(minutes=5), fence=held.attempts)
    ).held

    await as_b.complete(owned.run_id, output_json={"stolen": True}, fence=held.attempts)
    after_cross = await as_a.load(owned.run_id)
    out["cross_complete_status"] = None if after_cross is None else after_cross.status.value
    out["cross_complete_output"] = None if after_cross is None else after_cross.output_json

    await as_a.complete(owned.run_id, output_json={"ok": True}, fence=held.attempts)
    landed = await as_a.load(owned.run_id)
    out["own_complete_status"] = None if landed is None else landed.status.value
    out["own_complete_output"] = None if landed is None else landed.output_json

    # 1b. The other terminal verbs share ``_finish``, but they are separate port verbs and a
    #     predicate can be lost from one of them: ``fail`` is driven from the other side of
    #     the boundary too, over a second run so the first one's landing cannot mask it.
    second = await as_a.enqueue("owned-2", input_json=None)
    second_held = await as_a.begin(second.run_id, lease_for=timedelta(minutes=5))
    assert second_held is not None

    await as_b.fail(second.run_id, error="stolen", fence=second_held.attempts)
    after_cross_fail = await as_a.load(second.run_id)
    out["cross_fail_status"] = None if after_cross_fail is None else after_cross_fail.status.value
    out["cross_fail_error"] = None if after_cross_fail is None else after_cross_fail.error

    # 2. An untagged run stays completable from anywhere — the arm that keeps it out of a
    #    reclaim loop — and its output is still readable, which is what makes that safe.
    orphan = await unbound.enqueue("orphan", input_json={"t": None})
    orphan_held = await as_b.begin(orphan.run_id, lease_for=timedelta(minutes=5))
    out["orphan_begin"] = orphan_held is not None

    assert orphan_held is not None
    # …and its holder can keep the lease alive, which is the verb a long body leans on: an
    # untagged run whose renewal were refused would be reclaimed mid-flight.
    out["orphan_renew"] = (
        await as_b.renew(orphan.run_id, lease_for=timedelta(minutes=5), fence=orphan_held.attempts)
    ).held

    await as_b.complete(orphan.run_id, output_json={"ok": True}, fence=orphan_held.attempts)
    orphan_after = await unbound.load(orphan.run_id)
    out["orphan_status"] = None if orphan_after is None else orphan_after.status.value
    out["orphan_output"] = None if orphan_after is None else orphan_after.output_json

    # 3. …and is invisible to a bound listing, which matches the tenant exactly. Widening an
    #    enumeration is a disclosure; the arm above is a liveness fix. Different questions.
    listed = await cast("DurableRunAdminPort", as_b).list_runs(limit=50)
    out["orphan_listed_by_b"] = [r.name for r in listed.records if r.name == "orphan"]

    # 4. An explicit tenant contradicting the binding is refused rather than half-applied.
    try:
        await as_a.enqueue("mismatched", input_json=None, tenant_id=tenant_b)
    except CoreException as error:
        out["enqueue_mismatch"] = error.kind.value
    else:  # pragma: no cover - a store that allows it is the failure being pinned
        out["enqueue_mismatch"] = "allowed"

    # 5. Naming a tenant where nothing is bound is honoured — relation included, which is
    #    what makes the run reachable by the tenant it was enqueued for.
    for_b = await unbound.enqueue("for-b", input_json={"t": "b"}, tenant_id=tenant_b)
    reached = await as_b.load(for_b.run_id)
    out["explicit_tenant_reaches_owner"] = None if reached is None else reached.name
    out["explicit_tenant_hidden_from_other"] = await as_a.load(for_b.run_id) is None

    # 6. The schedule store answers both halves the same way, over its scoped key.
    schedule_a = schedules_for(tenant_a)
    schedule_b = schedules_for(tenant_b)
    schedule_unbound = schedules_for(None)
    fire_at = utcnow() - timedelta(minutes=1)

    try:
        await schedule_a.put(
            DurableScheduleRecord(
                schedule_id="cross",
                name="fn",
                cron="0 3 * * *",
                next_fire_at=fire_at,
                tenant_id=tenant_b,
            )
        )
    except CoreException as error:
        out["schedule_mismatch"] = error.kind.value
    else:  # pragma: no cover - a store that allows it is the failure being pinned
        out["schedule_mismatch"] = "allowed"

    await schedule_unbound.put(
        DurableScheduleRecord(
            schedule_id="nightly-b",
            name="fn",
            cron="0 3 * * *",
            next_fire_at=fire_at,
            tenant_id=tenant_b,
        )
    )

    owner_sees = await schedule_b.load("nightly-b")
    out["schedule_reaches_owner"] = None if owner_sees is None else owner_sees.name
    out["schedule_hidden_from_other"] = await schedule_a.load("nightly-b") is None

    return out


# ....................... #


async def run_schedule_scenario(store: DurableScheduleStorePort) -> dict[str, Any]:
    """Drive the recurring-schedule store and collect plane-independent observables.

    Firing is made exactly-once by :meth:`advance`'s compare-and-set rather than by a lease,
    so the observable that matters is that a *stale* advance loses: two schedulers reading
    one due instant must produce one advance between them.
    """

    out: dict[str, Any] = {}
    fire_at = utcnow() - timedelta(minutes=1)
    later = fire_at + timedelta(hours=1)

    await store.put(
        DurableScheduleRecord(
            schedule_id="nightly",
            name="fn",
            cron="0 3 * * *",
            next_fire_at=fire_at,
            tz="UTC",
            input_json={"n": 1},
        )
    )

    loaded = await store.load("nightly")
    out["loaded_name"] = None if loaded is None else loaded.name
    out["loaded_cron"] = None if loaded is None else loaded.cron
    out["loaded_tz"] = None if loaded is None else loaded.tz
    out["loaded_input"] = None if loaded is None else loaded.input_json
    out["loaded_enabled"] = None if loaded is None else loaded.enabled

    due = await store.claim_due(now=utcnow(), limit=10)
    out["due_ids"] = sorted(record.schedule_id for record in due)

    out["advanced"] = await store.advance("nightly", from_fire_at=fire_at, to_fire_at=later)
    # The second scheduler read the same due instant and loses the compare-and-set, which is
    # what keeps one due instant to one run.
    out["advanced_again"] = await store.advance("nightly", from_fire_at=fire_at, to_fire_at=later)

    out["not_due_after_advance"] = [
        record.schedule_id for record in await store.claim_due(now=utcnow(), limit=10)
    ]

    # Re-putting the same id updates in place rather than adding a second schedule.
    await store.put(
        DurableScheduleRecord(
            schedule_id="nightly",
            name="fn",
            cron="*/5 * * * *",
            next_fire_at=fire_at,
            enabled=False,
        )
    )
    repuit = await store.load("nightly")
    out["reput_cron"] = None if repuit is None else repuit.cron
    out["paused_not_due"] = [
        record.schedule_id for record in await store.claim_due(now=utcnow(), limit=10)
    ]

    out["zero_limit_due"] = [
        record.schedule_id for record in await store.claim_due(now=utcnow(), limit=0)
    ]

    out["deleted"] = await store.delete("nightly")
    out["delete_again"] = await store.delete("nightly")
    out["load_after_delete"] = await store.load("nightly")

    return out
