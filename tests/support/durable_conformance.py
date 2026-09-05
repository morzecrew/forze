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

from forze.application.contracts.durable.function import (
    DurableFunctionStepPort,
    DurableRunAdminPort,
    DurableRunContext,
    DurableRunStatus,
    DurableRunStorePort,
    bind_durable_run,
    durable_run_control_capabilities,
    reset_durable_run,
)

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
