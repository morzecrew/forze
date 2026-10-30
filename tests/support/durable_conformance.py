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

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import attrs

from forze.application.contracts.crypto import (
    AesGcmAead,
    BytesCipherPort,
    DataKey,
    KeyManagementPort,
    KeyRef,
    TenantTemplateKeyDirectory,
)
from forze.application.contracts.durable.function import (
    REQUEUE_UNSCOPED_CODE,
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
from forze.application.integrations.crypto import PAYLOAD_CIPHER_MISSING_CODE, Keyring
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import utcnow
from forze_mock import MockKeyManagement, MockState

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


async def run_function_filter_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
) -> dict[str, Any]:
    """Drive a recovery scan restricted to some functions and collect what it claimed.

    What a worker serving only some functions relies on: it never claims a run of another
    name, neither a due one nor one whose lease lapsed, so that run waits for the worker that
    serves it. A scan without the filter still claims any name. Takes a factory because the
    filter must narrow a bound tenant's scan, not replace it.
    """

    out: dict[str, Any] = {}
    tenant = UUID("00000000-0000-0000-0000-00000000000a")
    lease = timedelta(minutes=5)
    unbound = store_for(None)

    for name in ("fast", "slow", "other"):
        await unbound.enqueue(name, input_json=None)

    await unbound.enqueue("fast", input_json=None, tenant_id=tenant)

    out["empty_selects_nothing"] = [
        record.name
        for record in await unbound.claim_abandoned(
            limit=10, lease_for=lease, functions=frozenset()
        )
    ]
    out["bound_scan"] = [
        (record.name, record.tenant_id == tenant)
        for record in await store_for(tenant).claim_abandoned(
            limit=10, lease_for=lease, functions=frozenset({"fast", "slow"})
        )
    ]
    out["filtered_scan"] = sorted(
        record.name
        for record in await unbound.claim_abandoned(
            limit=10, lease_for=lease, functions=frozenset({"fast", "unregistered"})
        )
    )

    # A zero lease leaves the claimed run abandoned at once, as a crashed worker would.
    out["lapsed_claim"] = [
        (record.name, record.attempts)
        for record in await unbound.claim_abandoned(
            limit=10, lease_for=timedelta(0), functions=frozenset({"slow"})
        )
    ]
    out["lapsed_left_to_its_worker"] = sorted(
        record.name
        for record in await unbound.claim_abandoned(
            limit=10, lease_for=lease, functions=frozenset({"other", "fast"})
        )
    )
    out["unfiltered_reclaims"] = [
        (record.name, record.attempts)
        for record in await unbound.claim_abandoned(limit=10, lease_for=lease)
    ]

    return out


FUNCTION_FILTER_OUTCOME: dict[str, Any] = {
    "empty_selects_nothing": [],
    "bound_scan": [("fast", True)],
    "filtered_scan": ["fast"],
    "lapsed_claim": [("slow", 1)],
    "lapsed_left_to_its_worker": ["other"],
    "unfiltered_reclaims": [("slow", 2)],
}
"""What :func:`run_function_filter_scenario` answers on every engine, the mock included."""


# ....................... #


async def run_retry_scenario(store: DurableRunStorePort) -> dict[str, Any]:
    """Send a running run back for a retry and collect plane-independent observables.

    ``mark_retry`` is the one write that returns a claimed run to ``PENDING``. It is guarded
    on ``RUNNING`` and fenced like a terminal write, keeps the run's id, payload and
    ``attempts``, and makes it due at the instant given: the scan leaves it alone until
    then, and the claim that takes it advances the fence past the worker that sent it back.
    """

    lease = timedelta(minutes=5)
    out: dict[str, Any] = {}

    later = await store.enqueue("retry-later", input_json={"n": 1})
    held = await store.begin(later.run_id, lease_for=lease)
    assert held is not None

    # A stale holder cannot send back a run it no longer holds.
    await store.mark_retry(
        later.run_id, error="stale", available_at=utcnow(), fence=held.attempts + 99
    )
    unmoved = await store.load(later.run_id)
    out["stale_retry_ignored"] = unmoved is not None and (unmoved.status.value, unmoved.error) == (
        "running",
        None,
    )

    await store.mark_retry(
        later.run_id,
        error="blip",
        available_at=utcnow() + timedelta(hours=1),
        fence=held.attempts,
    )
    waiting = await store.load(later.run_id)
    assert waiting is not None
    out["waiting"] = (waiting.status.value, waiting.attempts, waiting.error, waiting.input_json)
    out["waiting_is_delayed"] = waiting.available_at is not None and waiting.available_at > utcnow()

    # The lease went with it: the worker that sent it back holds nothing any more.
    renewal = await store.renew(later.run_id, lease_for=lease, fence=held.attempts)
    out["old_holder_renews"] = renewal.held

    # Only a running run goes back, so a second retry of the waiting one changes nothing.
    await store.mark_retry(later.run_id, error="again", available_at=utcnow(), fence=held.attempts)
    again = await store.load(later.run_id)
    out["retry_of_pending_ignored"] = again is not None and again.error == "blip"

    # A due retry is claimed by the scan under a fence one higher; the one not due is not.
    due = await store.enqueue("retry-due", input_json=None)
    due_held = await store.begin(due.run_id, lease_for=lease)
    assert due_held is not None
    await store.mark_retry(
        due.run_id,
        error="blip",
        available_at=utcnow() - timedelta(seconds=1),
        fence=due_held.attempts,
    )
    claimed = await store.claim_abandoned(limit=10, lease_for=lease)
    out["scan_claims"] = sorted((r.name, r.attempts, r.error) for r in claimed)

    # A run that already landed is not sent back.
    done = await store.enqueue("retry-done", input_json=None)
    done_held = await store.begin(done.run_id, lease_for=lease)
    assert done_held is not None
    await store.complete(done.run_id, output_json={"ok": True}, fence=done_held.attempts)
    await store.mark_retry(
        done.run_id, error="late", available_at=utcnow(), fence=done_held.attempts
    )
    finished = await store.load(done.run_id)
    out["terminal_not_retried"] = (
        None if finished is None else (finished.status.value, finished.error)
    )

    return out


RETRY_OUTCOME: dict[str, Any] = {
    "stale_retry_ignored": True,
    "waiting": ("pending", 1, "blip", {"n": 1}),
    "waiting_is_delayed": True,
    "old_holder_renews": False,
    "retry_of_pending_ignored": True,
    "scan_claims": [("retry-due", 2, "blip")],
    "terminal_not_retried": ("completed", None),
}
"""What :func:`run_retry_scenario` answers on every engine, the mock included."""


# ....................... #


async def run_requeue_failed_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
) -> dict[str, Any]:
    """Requeue landed runs from both sides of the tenant boundary.

    Only ``FAILED`` and ``TIMED_OUT`` runs move, back to a due ``PENDING`` under their own
    id, keeping ``attempts``, ``error`` and cancel stamps, so the next claim advances the
    fence and replays the journal. ``CANCELLED`` and ``FORWARD_INCOMPLETE`` runs stay, and so
    does a failed run whose cancel ask is unanswered. A call naming neither runs nor a
    function is refused, bound or not. *store_for* builds a store bound to a tenant
    (``None`` for unbound) over one relation.
    """

    tenant_a = UUID("00000000-0000-0000-0000-00000000000a")
    tenant_b = UUID("00000000-0000-0000-0000-00000000000b")
    as_a, as_b, unbound = store_for(tenant_a), store_for(tenant_b), store_for(None)
    admin_a, admin = cast(DurableRunAdminPort, as_a), cast(DurableRunAdminPort, unbound)
    lease = timedelta(minutes=5)

    async def landed(
        store: DurableRunStorePort,
        name: str,
        land: Callable[[DurableRunStorePort, str, int], Awaitable[None]],
        *,
        asked: bool = False,
        declined: bool = False,
    ) -> str:
        run = await store.enqueue(name, input_json={"n": name})
        held = await store.begin(run.run_id, lease_for=lease)
        assert held is not None

        if asked:
            await cast(DurableRunAdminPort, store).request_cancel(run.run_id)

        if declined:
            await store.refuse_cancel(run.run_id, fence=held.attempts)

        await land(store, run.run_id, held.attempts)

        return run.run_id

    async def fail(store: DurableRunStorePort, run_id: str, fence: int) -> None:
        await store.fail(run_id, error="boom", fence=fence)

    async def time_out(store: DurableRunStorePort, run_id: str, fence: int) -> None:
        await store.mark_timed_out(run_id, error="cap", fence=fence)

    async def cancel(store: DurableRunStorePort, run_id: str, fence: int) -> None:
        await store.mark_cancelled(run_id, fence=fence)

    async def forward(store: DurableRunStorePort, run_id: str, fence: int) -> None:
        await store.mark_forward_incomplete(run_id, error="pivot", fence=fence)

    failed_a = await landed(as_a, "fa", fail)
    timed_out_a = await landed(as_a, "ta", time_out)
    asked_a = await landed(as_a, "xa", fail, asked=True)
    declined_a = await landed(as_a, "ra", fail, asked=True, declined=True)
    await landed(as_a, "oa", fail)
    cancelled_a = await landed(as_a, "ca", cancel)
    forward_a = await landed(as_a, "wa", forward)
    await landed(as_a, "shared", fail)
    failed_b = await landed(as_b, "fb", fail)
    await landed(as_b, "shared", fail)
    await landed(unbound, "fn", fail)

    async def state(run_id: str) -> tuple[str, int, str | None, bool, bool] | None:
        record = await unbound.load(run_id)

        if record is None:
            return None

        return (
            record.status.value,
            record.attempts,
            record.error,
            record.cancel_requested_at is not None,
            record.available_at is None,
        )

    async def outcome(call: Awaitable[int]) -> Any:
        try:
            return await call

        except CoreException as error:
            return error.kind.value, error.code

    out: dict[str, Any] = {}

    # A run's failure history is not brought back whole: name the runs or the function.
    out["bound_unnamed"] = await outcome(admin_a.requeue_failed())
    out["unbound_unnamed"] = await outcome(admin.requeue_failed())

    out["named_across_tenants"] = await admin_a.requeue_failed(run_ids=[failed_b])
    out["named_none"] = await admin_a.requeue_failed(run_ids=[])
    out["by_name"] = await admin_a.requeue_failed(name="oa")
    out["by_ids"] = await admin_a.requeue_failed(
        run_ids=[failed_a, timed_out_a, asked_a, declined_a, cancelled_a, forward_a]
    )
    out["requeued"] = await state(failed_a)
    out["requeued_timed_out"] = await state(timed_out_a)
    out["requeued_declined"] = await state(declined_a)
    out["left"] = [
        await state(asked_a),
        await state(cancelled_a),
        await state(forward_a),
        await state(failed_b),
    ]

    out["unbound_named"] = await admin.requeue_failed(run_ids=[failed_b, "no-such-run"])
    out["unbound_by_name"] = await admin.requeue_failed(name="shared")
    out["requeued_again"] = await admin.requeue_failed(name="shared")

    out["claimed"] = sorted(
        (r.name, r.attempts) for r in await unbound.claim_abandoned(limit=20, lease_for=lease)
    )

    return out


REQUEUE_FAILED_OUTCOME: dict[str, Any] = {
    "bound_unnamed": ("validation", REQUEUE_UNSCOPED_CODE),
    "unbound_unnamed": ("validation", REQUEUE_UNSCOPED_CODE),
    "named_across_tenants": 0,
    "named_none": 0,
    "by_name": 1,
    "by_ids": 3,
    "requeued": ("pending", 1, "boom", False, True),
    "requeued_timed_out": ("pending", 1, "cap", False, True),
    "requeued_declined": ("pending", 1, "boom", True, True),
    "left": [
        ("failed", 1, "boom", True, True),
        ("cancelled", 1, None, False, True),
        ("forward_incomplete", 1, "pivot", False, True),
        ("failed", 1, "boom", False, True),
    ],
    "unbound_named": 1,
    "unbound_by_name": 2,
    "requeued_again": 0,
    "claimed": [
        ("fa", 2),
        ("fb", 2),
        ("oa", 2),
        ("ra", 2),
        ("shared", 2),
        ("shared", 2),
        ("ta", 2),
    ],
}
"""What :func:`run_requeue_failed_scenario` answers on every engine, the mock included."""


# ....................... #


@attrs.define
class _RecoveryKms:
    """A KMS that refuses one tenant's key for good, and another's for now."""

    gone: str
    """Pending deletion: a configuration error, as the cloud KMS adapters raise it."""

    throttled: str
    """Throttled: an infrastructure error, which passes on its own."""

    inner: MockKeyManagement = attrs.field(factory=MockKeyManagement)

    async def generate_data_key(self, key_ref: KeyRef) -> DataKey:
        return await self.inner.generate_data_key(key_ref)

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        if key_ref.key_id == self.gone:
            raise exc.configuration("Key is pending deletion", code="test.key_pending_deletion")

        if key_ref.key_id == self.throttled:
            raise exc.infrastructure("KMS request throttled", code="test.kms_throttled")

        return await self.inner.unwrap_data_key(wrapped=wrapped, key_ref=key_ref)


def _tenant_keyring(kms: KeyManagementPort) -> Keyring:
    return Keyring(
        kms=kms,
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(template="tenant-{tenant_id}", default_key_id="cmk"),
    )


async def run_unopenable_claim_scenario(
    store_for: Callable[[UUID | None, BytesCipherPort | None], DurableRunStorePort],
    lease_left: Callable[[str], Awaitable[timedelta]],
) -> dict[str, Any]:
    """Claim over sealed runs when one tenant's key cannot be opened.

    For the engines that seal payloads; the mock does not, so there is no mock leg.
    *store_for* builds a store bound to a tenant (``None`` for the unbound recovery scan)
    sealing with a cipher (``None`` for none); *lease_left* reads how long a run's lease has
    left, straight from the backend, since a run that could not be opened never comes back
    through the port. The recovery scan claims every tenant's runs in one statement and only
    then opens them, so a run whose key is gone must not fail the runs claimed with it, and
    it must be held out of the scan past its lease, or enough of them, oldest first, fill
    every batch the scan claims.
    """

    healthy, offboarded, throttled = uuid4(), uuid4(), uuid4()
    sealing = _tenant_keyring(MockKeyManagement())

    kept = await store_for(healthy, sealing).enqueue("kept", input_json={"n": 1})
    lost = await store_for(offboarded, sealing).enqueue("lost", input_json={"n": 2})
    busy = await store_for(throttled, sealing).enqueue("busy", input_json={"n": 3})

    # Taken by a worker that died, then asked to stop: its lease has lapsed, the stamp is on.
    # Unread, it might be a saga past its pivot, so it is held rather than cancelled.
    offboarded_store = store_for(offboarded, sealing)
    stopped = await offboarded_store.enqueue("stopped", input_json={"n": 4})
    await offboarded_store.begin(stopped.run_id, lease_for=timedelta(0))
    await cast(DurableRunAdminPort, offboarded_store).request_cancel(stopped.run_id)

    recovery_kms = _RecoveryKms(gone=f"tenant-{offboarded}", throttled=f"tenant-{throttled}")
    recovery = store_for(None, _tenant_keyring(recovery_kms))
    lease = timedelta(minutes=5)
    claimed = await recovery.claim_abandoned(limit=10, lease_for=lease)

    out: dict[str, Any] = {
        "claimed": [
            (record.name, record.input_json, record.payloads_unreadable) for record in claimed
        ],
        "kept_fence": next(r.attempts for r in claimed if r.run_id == kept.run_id),
        "kept_leased_for_its_lease": await lease_left(kept.run_id) <= lease,
        "lost_held_past_its_lease": await lease_left(lost.run_id) > lease,
        "busy_leased_for_its_lease": await lease_left(busy.run_id) <= lease,
        # Held like any other: unread, a saga past its pivot cannot be told from a run the
        # runner may stop, so the cancel waits until the run can be decrypted.
        "stopped_held_past_its_lease": await lease_left(stopped.run_id) > lease,
    }

    # A missing cipher is the deployment's, not one run's: every sealed run would be
    # skipped, so the scan refuses instead.
    await store_for(healthy, sealing).enqueue("unwired", input_json={"n": 3})

    try:
        await store_for(None, None).claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        out["no_cipher"] = None

    except CoreException as error:
        out["no_cipher"] = error.code

    return out


async def run_unreadable_listing_scenario(
    store_for: Callable[[UUID | None, BytesCipherPort | None], DurableRunStorePort],
) -> dict[str, Any]:
    """List runs when one tenant's key cannot open its run.

    A listing is how an operator finds a run to cancel, and how a quiesce asks whether any
    run is left in a state, so a run whose payloads cannot be decrypted is listed without
    them, flagged, rather than failing the page. A missing cipher still fails it.
    """

    healthy, offboarded = uuid4(), uuid4()
    sealing = _tenant_keyring(MockKeyManagement())

    await store_for(healthy, sealing).enqueue("kept", input_json={"n": 1})
    await store_for(offboarded, sealing).enqueue("lost", input_json={"n": 2})

    kms = _RecoveryKms(gone=f"tenant-{offboarded}", throttled="none")
    admin = cast(DurableRunAdminPort, store_for(None, _tenant_keyring(kms)))
    page = await admin.list_runs(limit=10)

    out: dict[str, Any] = {
        "listed": sorted(
            (record.name, record.input_json, record.payloads_unreadable) for record in page.records
        ),
    }

    # A passing fault fails the page rather than costing a KMS call per row.
    throttled = uuid4()
    await store_for(throttled, sealing).enqueue("busy", input_json={"n": 3})
    kms = _RecoveryKms(gone=f"tenant-{offboarded}", throttled=f"tenant-{throttled}")

    try:
        await cast(DurableRunAdminPort, store_for(None, _tenant_keyring(kms))).list_runs(limit=10)
        out["passing_fault"] = None

    except CoreException as error:
        out["passing_fault"] = error.code

    try:
        await cast(DurableRunAdminPort, store_for(None, None)).list_runs(limit=10)
        out["no_cipher"] = None

    except CoreException as error:
        out["no_cipher"] = error.code

    return out


async def run_quarantine_scenario(
    store_for: Callable[[UUID | None, BytesCipherPort | None], DurableRunStorePort],
    expire: Callable[[str], Awaitable[None]],
    lease_left: Callable[[str], Awaitable[timedelta | None]],
) -> dict[str, Any]:
    """Quarantine a sealed run recovery keeps failing to open, then bring it back.

    For the engines that seal payloads. The scan holds such a run out for a hold doubling
    with each claim; once that hold has reached its cap the run is quarantined instead, a
    status the scan never claims, so however many pile up they cannot crowd it. A lease of
    half a day doubles to the cap on the first claim, which drives the predicate in two scans
    rather than nine and shows the first claim is still only held. *expire* lapses a run's
    lease or hold, and *lease_left* reads it straight from the backend (``None`` when it is
    cleared).
    """

    offboarded, throttled = uuid4(), uuid4()
    sealing = _tenant_keyring(MockKeyManagement())

    lost = await store_for(offboarded, sealing).enqueue("lost", input_json={"n": 1})
    busy = await store_for(throttled, sealing).enqueue("busy", input_json={"n": 2})

    lease = timedelta(hours=12)
    failing = store_for(
        None,
        _tenant_keyring(_RecoveryKms(gone=f"tenant-{offboarded}", throttled=f"tenant-{throttled}")),
    )
    healthy = store_for(None, _tenant_keyring(MockKeyManagement()))
    # Reads the lost run without its payloads and every other run with them, so a listing
    # is not failed by the throttled one.
    listing = cast(
        DurableRunAdminPort,
        store_for(None, _tenant_keyring(_RecoveryKms(gone=f"tenant-{offboarded}", throttled=""))),
    )

    async def lost_state() -> tuple[str, int, bool]:
        page = await listing.list_runs(limit=10)
        record = next(r for r in page.records if r.run_id == lost.run_id)

        return record.status.value, record.attempts, record.payloads_unreadable

    async def quarantined() -> list[str]:
        page = await listing.list_runs(status=DurableRunStatus.QUARANTINED, limit=10)

        return sorted(record.name for record in page.records)

    out: dict[str, Any] = {}

    # 1. The first claim holds, whatever the lease.
    out["first_claimed"] = [
        r.name for r in await failing.claim_abandoned(limit=10, lease_for=lease)
    ]
    out["first_lost"] = await lost_state()
    out["first_lost_held_past_its_lease"] = (await lease_left(lost.run_id) or lease) > lease

    # 2. The second claim's doubled hold is past the cap: quarantined, its lease cleared. The
    #    throttled run's fault passes, so however far its hold doubles it keeps its lease.
    await expire(lost.run_id)
    await expire(busy.run_id)

    out["second_claimed"] = [
        r.name for r in await failing.claim_abandoned(limit=10, lease_for=lease)
    ]
    out["second_lost"] = await lost_state()
    out["second_lost_lease"] = await lease_left(lost.run_id)
    out["busy_kept_its_lease"] = (await lease_left(busy.run_id) or lease * 2) <= lease
    out["quarantined"] = await quarantined()
    page = await listing.list_runs(status=DurableRunStatus.QUARANTINED, limit=10)
    out["quarantined_unreadable"] = [r.payloads_unreadable for r in page.records]

    # 3. The scan never claims it, even one that could now open it.
    await expire(busy.run_id)
    out["healthy_scan"] = [r.name for r in await healthy.claim_abandoned(limit=10, lease_for=lease)]
    out["healthy_scan_lost"] = await lost_state()

    # 4. Requeue is scoped like the other admin verbs: another tenant's moves nothing.
    other = cast(DurableRunAdminPort, store_for(throttled, None))
    owner = cast(DurableRunAdminPort, store_for(offboarded, None))
    out["requeued_by_other"] = await other.requeue_quarantined()
    out["after_other"] = await quarantined()
    out["requeued_by_owner"] = await owner.requeue_quarantined()
    out["requeued_lost"] = await lost_state()
    out["requeued_lost_lease"] = await lease_left(lost.run_id)

    # 5. Its key still gone, the next claim quarantines it again at once.
    out["requeued_claimed"] = [
        r.name for r in await failing.claim_abandoned(limit=10, lease_for=lease)
    ]
    out["requarantined"] = await lost_state()

    # 6. A missing cipher fails the scan rather than quarantining what it cannot open.
    out["requeued_once_more"] = await owner.requeue_quarantined()

    try:
        await store_for(None, None).claim_abandoned(limit=10, lease_for=lease)
        out["no_cipher"] = None

    except CoreException as error:
        out["no_cipher"] = error.code

    out["no_cipher_lost"] = await lost_state()

    # 7. With its key back, the run is claimed and opened.
    await expire(lost.run_id)
    out["recovered"] = [
        (r.name, r.input_json, r.attempts)
        for r in await healthy.claim_abandoned(limit=10, lease_for=lease)
    ]

    return out


QUARANTINE_OUTCOME: dict[str, Any] = {
    "first_claimed": [],
    "first_lost": ("running", 1, True),
    "first_lost_held_past_its_lease": True,
    "second_claimed": [],
    "second_lost": ("quarantined", 2, True),
    "second_lost_lease": None,
    "busy_kept_its_lease": True,
    "quarantined": ["lost"],
    "quarantined_unreadable": [True],
    "healthy_scan": ["busy"],
    "healthy_scan_lost": ("quarantined", 2, True),
    "requeued_by_other": 0,
    "after_other": ["lost"],
    "requeued_by_owner": 1,
    "requeued_lost": ("pending", 2, True),
    "requeued_lost_lease": None,
    "requeued_claimed": [],
    "requarantined": ("quarantined", 3, True),
    "requeued_once_more": 1,
    "no_cipher": PAYLOAD_CIPHER_MISSING_CODE,
    "no_cipher_lost": ("running", 4, True),
    "recovered": [("lost", {"n": 1}, 5)],
}
"""What :func:`run_quarantine_scenario` answers on every engine that seals."""


class FirstCallRuns:
    """A client whose first call to *method* runs *hook* before it.

    Puts a competing scanner between a claim and the write that follows it, the one ordering
    a sequential test never produces on its own.
    """

    def __init__(self, inner: Any, method: str, hook: Callable[[], Awaitable[None]]) -> None:
        self._inner = inner
        self._method = method
        self._hook: Callable[[], Awaitable[None]] | None = hook

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._inner, name)

        if name != self._method or self._hook is None:
            return attribute

        hook, self._hook = self._hook, None

        async def call(*args: Any, **kwargs: Any) -> Any:
            await hook()

            return await attribute(*args, **kwargs)

        return call


async def run_quarantine_fence_scenario(
    store_for: Callable[[UUID | None, BytesCipherPort | None], DurableRunStorePort],
    racing: Callable[[BytesCipherPort, Callable[[], Awaitable[None]]], DurableRunStorePort],
    expire: Callable[[str], Awaitable[None]],
) -> dict[str, Any]:
    """A scan quarantines only a run it still holds.

    Between the claim that failed to open a run and the write that quarantines it, the run's
    lease can lapse and another scanner, its key back, reclaim it. The run is then that
    scanner's, so the stale write must move nothing. *racing* builds an unbound store sealing
    with a cipher, whose first write after its claim runs the given hook first.
    """

    offboarded = uuid4()
    lost = await store_for(offboarded, _tenant_keyring(MockKeyManagement())).enqueue(
        "lost", input_json={"n": 1}
    )
    lease = timedelta(hours=12)
    failing = _tenant_keyring(_RecoveryKms(gone=f"tenant-{offboarded}", throttled=""))
    healthy = store_for(None, _tenant_keyring(MockKeyManagement()))

    # The first claim only holds it, so the next one would quarantine it.
    await store_for(None, failing).claim_abandoned(limit=10, lease_for=lease)
    await expire(lost.run_id)

    reclaimed: list[int] = []

    async def reclaim() -> None:
        await expire(lost.run_id)
        claimed = await healthy.claim_abandoned(limit=10, lease_for=lease)
        reclaimed.extend(record.attempts for record in claimed)

    await racing(failing, reclaim).claim_abandoned(limit=10, lease_for=lease)
    after = await healthy.load(lost.run_id)

    return {
        "reclaimed": reclaimed,
        "after": None if after is None else (after.status.value, after.attempts),
    }


async def run_quarantine_admin_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
    quarantine: Callable[[str], Awaitable[None]],
) -> dict[str, Any]:
    """List, cancel and requeue quarantined runs, from both sides of the tenant boundary.

    Every engine answers these alike, sealing or not, so the leg puts a claimed run into
    quarantine directly through *quarantine*: the mock never seals, so it never quarantines
    a run itself, and what is compared is how each one treats the status once a run is in
    it. *store_for* builds a store bound to a tenant (``None`` for unbound) over one relation.
    """

    tenant_a = UUID("00000000-0000-0000-0000-00000000000a")
    tenant_b = UUID("00000000-0000-0000-0000-00000000000b")
    as_a, as_b, unbound = store_for(tenant_a), store_for(tenant_b), store_for(None)
    admin_a, admin = cast(DurableRunAdminPort, as_a), cast(DurableRunAdminPort, unbound)
    lease = timedelta(minutes=5)

    held_a = await as_a.enqueue("qa", input_json=None)
    held_b = await as_b.enqueue("qb", input_json=None)
    untagged = await unbound.enqueue("qn", input_json=None)
    # ``begin`` takes a run before it is due, so one can reach quarantine still delayed.
    delayed = await as_a.enqueue("qd", input_json=None, available_at=utcnow() + timedelta(hours=1))

    for store, run in ((as_a, held_a), (as_b, held_b), (unbound, untagged), (as_a, delayed)):
        assert await store.begin(run.run_id, lease_for=lease) is not None
        await quarantine(run.run_id)

    async def names(port: DurableRunAdminPort) -> list[str]:
        page = await port.list_runs(status=DurableRunStatus.QUARANTINED, limit=10)

        return sorted(record.name for record in page.records)

    async def state(run_id: str) -> tuple[str, int, bool] | None:
        record = await unbound.load(run_id)

        if record is None:
            return None

        return record.status.value, record.attempts, record.cancel_requested_at is not None

    out: dict[str, Any] = {}

    out["listed"] = await names(admin)
    out["listed_by_a"] = await names(admin_a)
    out["scan_claims"] = [r.name for r in await unbound.claim_abandoned(limit=10, lease_for=lease)]
    out["begin_claims"] = await unbound.begin(held_a.run_id, lease_for=lease) is not None

    # A cancel is stamped and waits: unread, a saga past its pivot cannot be told apart.
    out["ask"] = await admin_a.request_cancel(held_a.run_id)
    out["asked"] = await state(held_a.run_id)
    out["ask_across_tenants"] = await admin_a.request_cancel(held_b.run_id)

    out["requeued_by_a"] = await admin_a.requeue_quarantined()
    out["left_after_a"] = await names(admin)
    out["requeued_a"] = await state(held_a.run_id)
    out["named_across_tenants"] = await admin_a.requeue_quarantined(run_ids=[held_b.run_id])

    # Unbound and unnamed, a requeue would bring back every tenant's runs, those whose key is
    # gone for good among them, ahead of every fresh run; it is refused while any is tagged.
    try:
        await admin.requeue_quarantined()
        out["unbound_unnamed"] = "allowed"

    except CoreException as error:
        out["unbound_unnamed"] = (error.kind.value, error.code)

    out["left_after_refusal"] = await names(admin)
    out["named_none"] = await admin.requeue_quarantined(run_ids=[])
    out["named"] = await admin.requeue_quarantined(run_ids=[held_b.run_id, "no-such-run"])

    # With no tagged run left in quarantine, as in a single-tenant deployment, it proceeds.
    out["untagged_unnamed"] = await admin.requeue_quarantined()
    out["requeued_again"] = await admin.requeue_quarantined()

    # A requeued run has been claimed before, so it may be a saga past its pivot: a cancel
    # asked of it now is stamped and left for the runner, not landed at once.
    out["ask_requeued"] = await admin.request_cancel(held_b.run_id)
    out["asked_requeued"] = await state(held_b.run_id)

    out["claimed"] = sorted(
        (r.name, r.attempts, r.cancel_requested_at is not None)
        for r in await unbound.claim_abandoned(limit=10, lease_for=lease)
    )

    return out


async def run_requeue_race_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
    racing: Callable[[Callable[[], Awaitable[None]]], DurableRunStorePort],
    quarantine: Callable[[str], Awaitable[None]],
) -> dict[str, Any]:
    """An unbound, unnamed requeue moves untagged runs only, even past its own check.

    Between the check that no quarantined run is tagged and the update, a tenant's run can be
    quarantined; the update must leave it where it is. *racing* builds an unbound store whose
    write after its check runs the given hook first.
    """

    lease = timedelta(minutes=5)
    unbound = store_for(None)
    untagged = await unbound.enqueue("qn", input_json=None)
    assert await unbound.begin(untagged.run_id, lease_for=lease) is not None
    await quarantine(untagged.run_id)

    tagged_store = store_for(UUID("00000000-0000-0000-0000-00000000000a"))
    tagged = await tagged_store.enqueue("qa", input_json=None)
    assert await tagged_store.begin(tagged.run_id, lease_for=lease) is not None

    async def quarantine_tagged() -> None:
        await quarantine(tagged.run_id)

    moved = await cast(DurableRunAdminPort, racing(quarantine_tagged)).requeue_quarantined()
    page = await cast(DurableRunAdminPort, unbound).list_runs(
        status=DurableRunStatus.QUARANTINED, limit=10
    )

    return {"moved": moved, "still_quarantined": [record.name for record in page.records]}


QUARANTINE_ADMIN_OUTCOME: dict[str, Any] = {
    "listed": ["qa", "qb", "qd", "qn"],
    "listed_by_a": ["qa", "qd"],
    "scan_claims": [],
    "begin_claims": False,
    "ask": True,
    "asked": ("quarantined", 1, True),
    "ask_across_tenants": False,
    "requeued_by_a": 2,
    "left_after_a": ["qb", "qn"],
    "requeued_a": ("pending", 1, True),
    "named_across_tenants": 0,
    "unbound_unnamed": ("validation", REQUEUE_UNSCOPED_CODE),
    "left_after_refusal": ["qb", "qn"],
    "named_none": 0,
    "named": 1,
    "untagged_unnamed": 1,
    "requeued_again": 0,
    "ask_requeued": True,
    "asked_requeued": ("pending", 1, True),
    "claimed": [("qa", 2, True), ("qb", 2, True), ("qd", 2, False), ("qn", 2, False)],
}
"""What :func:`run_quarantine_admin_scenario` answers on every engine, the mock included."""


def mock_quarantine(state: MockState) -> Callable[[str], Awaitable[None]]:
    """Put a run of *state* into quarantine, as an engine's recovery scan would."""

    async def quarantine(run_id: str) -> None:
        state.durable_runs[run_id].update(status="quarantined", leased_until=None)

    return quarantine


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

    await as_b.mark_retry(
        second.run_id, error="stolen", available_at=utcnow(), fence=second_held.attempts
    )
    after_cross_retry = await as_a.load(second.run_id)
    out["cross_retry_status"] = (
        None if after_cross_retry is None else after_cross_retry.status.value
    )

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


async def run_purge_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
    step_of: Callable[[], DurableFunctionStepPort],
) -> dict[str, Any]:
    """Purge finished runs past a window and collect plane-independent observables.

    Only ``COMPLETED``, ``CANCELLED``, ``FAILED`` and ``TIMED_OUT`` runs go, oldest first and
    at most *limit* per call, each with its step journal; a run that still owes work stays,
    with its journal. A purged run frees its idempotency key. *store_for* must hand out
    stores wired to the journal *step_of* writes.
    """

    lease = timedelta(minutes=5)
    past = timedelta(microseconds=1)
    tenant = UUID("00000000-0000-0000-0000-00000000000a")
    store = store_for(None)
    admin = cast(DurableRunAdminPort, store)
    out: dict[str, Any] = {}

    async def held(name: str, **enqueue: Any) -> tuple[str, int]:
        run = await store.enqueue(name, input_json=None, **enqueue)
        claimed = await store.begin(run.run_id, lease_for=lease)
        assert claimed is not None
        return run.run_id, claimed.attempts

    # The batches go oldest landing first, and Mongo keeps milliseconds: a tie would let
    # it pick either run, so each landing below waits out the last one's millisecond.
    landing_gap = 0.002

    done, fence = await held("done", idempotency_key="k")
    await _journaled(step_of, done)
    await store.complete(done, output_json={"ok": True}, fence=fence)
    await asyncio.sleep(landing_gap)

    failed, fence = await held("failed")
    await store.fail(failed, error="boom", fence=fence)
    await asyncio.sleep(landing_gap)

    timed, fence = await held("timed")
    await store.mark_timed_out(timed, error="deadline", fence=fence)
    await asyncio.sleep(landing_gap)

    cancelled = (await store.enqueue("cancelled", input_json=None)).run_id
    await admin.request_cancel(cancelled)

    forward, fence = await held("forward")
    await _journaled(step_of, forward)
    await store.mark_forward_incomplete(forward, error="pivot", fence=fence)

    pending = (await store.enqueue("pending", input_json=None)).run_id
    running, _ = await held("running")

    owned, fence = await held("owned", tenant_id=tenant)
    await store.complete(owned, output_json=None, fence=fence)

    out["within_window"] = await admin.purge_finished(timedelta(hours=1))
    out["other_name"] = await admin.purge_finished(past, name="nope")

    refused: list[str] = []

    for statuses, older_than, limit in (
        ([DurableRunStatus.RUNNING], past, 10),
        ([DurableRunStatus.FORWARD_INCOMPLETE], past, 10),
        ([DurableRunStatus.QUARANTINED], past, 10),
        ([DurableRunStatus.COMPLETED], timedelta(0), 10),
        ([DurableRunStatus.COMPLETED], past, 0),
    ):
        try:
            await admin.purge_finished(older_than, statuses=statuses, limit=limit)
        except CoreException as error:
            refused.append(error.kind.value)

    out["refused"] = refused

    # Bound, only the tenant's own finished run goes.
    out["bound_purge"] = await cast(DurableRunAdminPort, store_for(tenant)).purge_finished(past)
    out["owned_gone"] = await store.load(owned) is None

    out["first_batch"] = await admin.purge_finished(past, limit=2)
    out["after_first_batch"] = [
        name
        for name, run_id in (("done", done), ("failed", failed), ("timed", timed))
        if await store.load(run_id) is not None
    ]
    out["rest"] = await admin.purge_finished(past)

    survivors = [done, failed, timed, cancelled, forward, pending, running]
    loaded = [await store.load(run_id) for run_id in survivors]
    out["statuses_left"] = [None if r is None else r.status.value for r in loaded]

    out["purged_journal_reruns"] = await _journaled(step_of, done)
    out["kept_journal_reruns"] = await _journaled(step_of, forward)

    again = await store.enqueue("done", input_json=None, idempotency_key="k")
    out["key_freed"] = again.run_id != done

    return out


PURGE_OUTCOME: dict[str, Any] = {
    "within_window": 0,
    "other_name": 0,
    "refused": ["validation"] * 5,
    "bound_purge": 1,
    "owned_gone": True,
    "first_batch": 2,
    "after_first_batch": ["timed"],
    "rest": 2,
    "statuses_left": [None, None, None, None, "forward_incomplete", "pending", "running"],
    "purged_journal_reruns": 1,
    "kept_journal_reruns": 0,
    "key_freed": True,
}
"""What :func:`run_purge_scenario` answers on every engine, the mock included."""


# ....................... #


async def run_coalesce_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
) -> dict[str, Any]:
    """Coalesce submits onto the run waiting unclaimed under one key.

    A submit joins a ``PENDING`` run nothing has claimed, keeping its input and due time; a
    claimed, retrying, cancelled or other tenant's run is never joined, so the submit after
    a claim records a new run. Twenty concurrent submits of one key converge on one run.
    """

    lease = timedelta(minutes=5)
    store = store_for(None)
    # Whole seconds: Mongo keeps milliseconds, and the joined run is read back from it.
    later = utcnow().replace(microsecond=0) + timedelta(hours=1)
    out: dict[str, Any] = {}

    first = await store.enqueue("fn", input_json={"n": 1}, coalesce_key="x", available_at=later)
    joined = await store.enqueue("fn", input_json={"n": 2}, coalesce_key="x")
    out["burst_joins"] = joined.run_id == first.run_id
    out["joined_keeps_input_and_due"] = (
        joined.input_json == {"n": 1} and joined.available_at == first.available_at
    )

    tenant = UUID("00000000-0000-0000-0000-00000000000b")
    other = await store_for(tenant).enqueue("fn", input_json=None, coalesce_key="x")
    out["other_tenant_distinct"] = other.run_id != first.run_id
    # An untagged key spelled like the tenant's scoped one must not reach the tenant's run.
    lookalike = await store.enqueue("fn", input_json=None, coalesce_key=f"{tenant}:x")
    out["lookalike_key_distinct"] = lookalike.run_id != other.run_id

    claimed = await store.begin(first.run_id, lease_for=lease)
    assert claimed is not None
    after_claim = await store.enqueue("fn", input_json=None, coalesce_key="x")
    out["after_claim_is_new"] = after_claim.run_id != first.run_id
    out["after_claim_joined"] = (
        await store.enqueue("fn", input_json=None, coalesce_key="x")
    ).run_id == after_claim.run_id

    await cast(DurableRunAdminPort, store).request_cancel(after_claim.run_id)
    after_cancel = await store.enqueue("fn", input_json=None, coalesce_key="x")
    out["after_cancel_is_new"] = after_cancel.run_id != after_claim.run_id

    retrying = await store.begin(after_cancel.run_id, lease_for=lease)
    assert retrying is not None
    await store.mark_retry(
        after_cancel.run_id, error="blip", available_at=utcnow(), fence=retrying.attempts
    )
    out["retrying_not_joined"] = (
        await store.enqueue("fn", input_json=None, coalesce_key="x")
    ).run_id != after_cancel.run_id

    try:
        await store.enqueue("fn", input_json=None, idempotency_key="k", coalesce_key="y")
    except CoreException as error:
        out["both_keys"] = error.kind.value
    else:  # pragma: no cover - a store that allows it is the failure being pinned
        out["both_keys"] = "allowed"

    burst = await asyncio.gather(
        *(store.enqueue("fn", input_json=None, coalesce_key="race") for _ in range(20))
    )
    out["concurrent_runs"] = len({record.run_id for record in burst})

    return out


COALESCE_OUTCOME: dict[str, Any] = {
    "burst_joins": True,
    "joined_keeps_input_and_due": True,
    "other_tenant_distinct": True,
    "lookalike_key_distinct": True,
    "after_claim_is_new": True,
    "after_claim_joined": True,
    "after_cancel_is_new": True,
    "retrying_not_joined": True,
    "both_keys": "validation",
    "concurrent_runs": 1,
}
"""What :func:`run_coalesce_scenario` answers on every engine, the mock included."""


# ....................... #

_LOOKALIKE_TENANT = UUID("00000000-0000-0000-0000-00000000000b")


async def run_lookalike_scenario(
    store_for: Callable[[UUID | None], DurableRunStorePort],
    schedules_for: Callable[[UUID | None], DurableScheduleStorePort],
) -> dict[str, Any]:
    """An untagged idempotency key or schedule id spelled like a tenant's scoped one.

    A shared store keeps every tenant's keys in one key space, so only the encoding keeps
    them apart: the untagged caller must reach nothing of the tenant's — not its run, not
    its schedule — and every key must read back as its caller wrote it.
    """

    tenant = _LOOKALIKE_TENANT
    unbound = store_for(None)
    out: dict[str, Any] = {}

    owned = await store_for(tenant).enqueue("fn", input_json=None, idempotency_key="k")
    untagged = await unbound.enqueue("fn", input_json=None, idempotency_key=f"{tenant}:k")
    again = await unbound.enqueue("fn", input_json=None, idempotency_key=f"{tenant}:k")
    out["idempotency_distinct"] = untagged.run_id != owned.run_id
    out["idempotency_converges"] = again.run_id == untagged.run_id
    loaded = [await unbound.load(run_id) for run_id in (owned.run_id, untagged.run_id)]
    out["idempotency_keys"] = [
        None if record is None else record.idempotency_key for record in loaded
    ] + [again.idempotency_key]
    # The tenant reaches the untagged run (the worker arm) and reads its key by the run's own
    # tenant, not by the binding.
    seen_by_tenant = await store_for(tenant).load(untagged.run_id)
    out["bound_load_key"] = None if seen_by_tenant is None else seen_by_tenant.idempotency_key

    owner = schedules_for(tenant)
    schedules = schedules_for(None)
    fire_at = utcnow().replace(microsecond=0) - timedelta(minutes=1)

    def schedule(schedule_id: str, name: str) -> DurableScheduleRecord:
        return DurableScheduleRecord(
            schedule_id=schedule_id, name=name, cron="0 3 * * *", next_fire_at=fire_at
        )

    await owner.put(schedule("s", "owned"))
    out["schedule_unseen"] = await schedules.load(f"{tenant}:s") is None

    await schedules.put(schedule(f"{tenant}:s", "untagged"))
    kept = await owner.load("s")
    out["schedule_owner_kept"] = None if kept is None else kept.name

    async def due(store: DurableScheduleStorePort) -> list[tuple[str, str]]:
        claimed = await store.claim_due(now=utcnow(), limit=10)

        return sorted((record.schedule_id, record.name) for record in claimed)

    out["due"] = await due(schedules)
    out["bound_due"] = await due(owner)

    # Each side moves only its own row: the tenant's advance leaves the lookalike due, and
    # each side's input lands on its own schedule.
    later = fire_at + timedelta(days=1)
    out["owner_advanced"] = await owner.advance("s", from_fire_at=fire_at, to_fire_at=later)
    out["due_after_owner_advance"] = await due(schedules)
    out["lookalike_advanced"] = await schedules.advance(
        f"{tenant}:s", from_fire_at=fire_at, to_fire_at=later
    )

    for store, schedule_id, by in ((owner, "s", "owned"), (schedules, f"{tenant}:s", "untagged")):
        await store.set_input(
            schedule_id, expected_cron="0 3 * * *", expected_tz=None, input_json={"by": by}
        )

    inputs = [await owner.load("s"), await schedules.load(f"{tenant}:s")]
    out["inputs"] = [None if record is None else record.input_json for record in inputs]

    out["delete_lookalike"] = await schedules.delete(f"{tenant}:s")
    out["owner_survives_delete"] = await owner.load("s") is not None

    return out


LOOKALIKE_OUTCOME: dict[str, Any] = {
    "idempotency_distinct": True,
    "idempotency_converges": True,
    "idempotency_keys": ["k", f"{_LOOKALIKE_TENANT}:k", f"{_LOOKALIKE_TENANT}:k"],
    "bound_load_key": f"{_LOOKALIKE_TENANT}:k",
    "schedule_unseen": True,
    "schedule_owner_kept": "owned",
    "due": [(f"{_LOOKALIKE_TENANT}:s", "untagged"), ("s", "owned")],
    "bound_due": [("s", "owned")],
    "owner_advanced": True,
    "due_after_owner_advance": [(f"{_LOOKALIKE_TENANT}:s", "untagged")],
    "lookalike_advanced": True,
    "inputs": [{"by": "owned"}, {"by": "untagged"}],
    "delete_lookalike": True,
    "owner_survives_delete": True,
}
"""What :func:`run_lookalike_scenario` answers on every engine, the mock included."""


# ....................... #


async def _journaled(step_of: Callable[[], DurableFunctionStepPort], run_id: str) -> int:
    """Run step ``s1`` of *run_id*; return 1 when its body ran, 0 when it replayed."""

    calls: list[int] = []

    async def work() -> int:
        calls.append(1)
        return 1

    token = bind_durable_run(DurableRunContext(run_id=run_id, name="fn"))
    try:
        await step_of().run("s1", work)
    finally:
        reset_durable_run(token)

    return len(calls)


async def run_purge_window_scenario(store: DurableRunStorePort) -> dict[str, Any]:
    """The purge window starts when a run lands, not when it was submitted.

    A run submitted longer ago than the window and finished just now is kept, so its
    idempotency key still converges a duplicate submit; once it has been landed for the
    window, it goes. Real waits of a fraction of a second, on whichever clock the engine
    stamps its landings with.
    """

    admin = cast(DurableRunAdminPort, store)
    window = timedelta(milliseconds=300)

    run = await store.enqueue("late", input_json=None, idempotency_key="late")
    claimed = await store.begin(run.run_id, lease_for=timedelta(minutes=5))
    assert claimed is not None
    await asyncio.sleep(0.4)  # older than the window by its submission...
    await store.complete(run.run_id, output_json=None, fence=claimed.attempts)

    out: dict[str, Any] = {"landed_just_now": await admin.purge_finished(window)}
    resubmit = await store.enqueue("late", input_json=None, idempotency_key="late")
    out["key_still_converges"] = resubmit.run_id == run.run_id

    await asyncio.sleep(0.4)  # ...and now by its landing too
    out["landed_past_the_window"] = await admin.purge_finished(window)

    return out


PURGE_WINDOW_OUTCOME: dict[str, Any] = {
    "landed_just_now": 0,
    "key_still_converges": True,
    "landed_past_the_window": 1,
}
"""What :func:`run_purge_window_scenario` answers on every engine, the mock included."""


async def run_purge_race_scenario(
    store: DurableRunStorePort,
    step_of: Callable[[], DurableFunctionStepPort],
    *,
    rounds: int = 20,
) -> dict[str, Any]:
    """Purge a failed run while an operator requeues it and a worker claims it.

    Whichever wins, the journal follows the run: a run deleted takes its steps, and a run
    requeued keeps them, so its next claim replays rather than re-runs them.
    """

    admin = cast(DurableRunAdminPort, store)
    lease = timedelta(minutes=5)
    held: list[bool] = []

    for _ in range(rounds):
        run = await store.enqueue("purge-race", input_json=None)
        claimed = await store.begin(run.run_id, lease_for=lease)
        assert claimed is not None
        await _journaled(step_of, run.run_id)
        await store.fail(run.run_id, error="boom", fence=claimed.attempts)

        async def requeue_and_claim(run_id: str = run.run_id) -> None:
            if await admin.requeue_failed(run_ids=[run_id]):
                await store.begin(run_id, lease_for=lease)

        await asyncio.gather(
            admin.purge_finished(timedelta(microseconds=1), name="purge-race"),
            requeue_and_claim(),
        )

        gone = await store.load(run.run_id) is None
        held.append(gone == bool(await _journaled(step_of, run.run_id)))

    return {"rounds": len(held), "journal_follows_run": all(held)}


async def run_coalesce_race_scenario(
    store: DurableRunStorePort,
    *,
    rounds: int = 30,
) -> dict[str, Any]:
    """Race a coalescing submit against the claim of the run it would join, round after round.

    Whichever lands first, the submit is covered: joined before the claim, the claimed run
    reads its change; after it, the submit is a new waiting run; while a join holds the row,
    the claim skips it and the run stays waiting. One unclaimed run per key, at most.
    """

    admin = cast(DurableRunAdminPort, store)
    lease = timedelta(minutes=5)
    waiting = await store.enqueue("coalesce-race", input_json=None, coalesce_key="r")
    held: list[bool] = []

    for _ in range(rounds):
        claimed, submitted = await asyncio.gather(
            store.begin(waiting.run_id, lease_for=lease),
            store.enqueue("coalesce-race", input_json=None, coalesce_key="r"),
        )
        joined = submitted.run_id == waiting.run_id
        page = await admin.list_runs(status=DurableRunStatus.PENDING, name="coalesce-race")
        unclaimed = [record for record in page.records if record.attempts == 0]

        # A claim skips the row only while the join holds it.
        held.append(
            (claimed is not None or joined)
            and len(unclaimed) == (0 if claimed is not None and joined else 1)
        )

        if claimed is not None:
            waiting = (
                await store.enqueue("coalesce-race", input_json=None, coalesce_key="r")
                if joined
                else submitted
            )

    return {"rounds": len(held), "invariant_held": all(held)}


async def run_schedule_input_race_scenario(store: DurableScheduleStorePort) -> dict[str, Any]:
    """Change only a schedule's input while other writers touch it between read and write.

    ``ensure_schedule`` reads the schedule, sees only the input differ, and writes. A fire and
    a pause landing between its read and its write must survive: a write of the record it
    read would put the fired instant back (a second fire) and un-pause the schedule. A
    re-registration under another cron or timezone landing there must not keep its cadence
    under the new input: a fire would then mix fields from two registrations.
    """

    from forze.application.contracts.durable.function import DurableScheduleStoreDepKey
    from forze.application.execution import Deps
    from forze.testing import context_from_deps
    from forze_kits.integrations.durable import DurableScheduler

    first = utcnow().replace(microsecond=0) + timedelta(hours=1)
    fired = first + timedelta(days=1)

    class _RaceAfterRead:
        def __init__(self, race: Callable[[DurableScheduleRecord], Awaitable[None]]) -> None:
            self.race = race
            self.raced = False

        def __getattr__(self, name: str) -> Any:
            return getattr(store, name)

        async def load(self, schedule_id: str) -> DurableScheduleRecord | None:
            record = await store.load(schedule_id)

            if record is not None and not self.raced:
                self.raced = True
                await self.race(record)

            return record

    async def ensure_racing(
        schedule_id: str, race: Callable[[DurableScheduleRecord], Awaitable[None]]
    ) -> DurableScheduleRecord:
        racing = _RaceAfterRead(race)
        ctx = context_from_deps(Deps.plain({DurableScheduleStoreDepKey: lambda _ctx: racing}))

        await store.put(
            DurableScheduleRecord(
                schedule_id=schedule_id,
                name="fn",
                cron="0 3 * * *",
                next_fire_at=first,
                input_json={"n": 1},
            )
        )
        await DurableScheduler().ensure_schedule(
            ctx, schedule_id, "fn", "0 3 * * *", input_json={"n": 2}
        )

        stored = await store.load(schedule_id)
        assert stored is not None

        return stored

    async def fire_and_pause(record: DurableScheduleRecord) -> None:
        await store.advance(record.schedule_id, from_fire_at=first, to_fire_at=fired)
        await store.put(attrs.evolve(record, next_fire_at=fired, enabled=False))

    stored = await ensure_racing("nightly", fire_and_pause)
    replaced = [
        await ensure_racing(
            f"replaced-{field}",
            lambda record, change={field: value}: store.put(attrs.evolve(record, **change)),
        )
        for field, value in (("cron", "0 4 * * *"), ("tz", "Europe/Berlin"))
    ]

    return {
        "input": stored.input_json,
        "kept_fire": stored.next_fire_at == fired,
        "kept_pause": stored.enabled is False,
        "replaced": [(record.cron, record.tz, record.input_json) for record in replaced],
    }


SCHEDULE_INPUT_RACE_OUTCOME: dict[str, Any] = {
    "input": {"n": 2},
    "kept_fire": True,
    "kept_pause": True,
    # The registration that wrote last wins whole, never a blend of the two.
    "replaced": [("0 3 * * *", None, {"n": 2})] * 2,
}
"""What :func:`run_schedule_input_race_scenario` answers on every engine, the mock included."""


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
