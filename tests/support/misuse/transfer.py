"""Transfer scripts — the P1 corpus re-expressed as backend-agnostic provocations.

Each :class:`~forze_dst.conformance.transfer.TransferScript` provokes the same seeded misuse its
corpus twin carries (the killing interleaving as a forced `Conductor` schedule, or a plain
re-invocation for the duplicate-delivery mutants) and reads the verdict back through the ports —
so the identical script runs on the mock and on real Postgres, and a verdict pair means the same
thing on both. Scripts are self-isolating on a reused store (fresh ids per invocation, the
anomaly-battery discipline), because real tables persist across scripts.

T2 is deliberately absent: its seeded effect is a trace-level marker (an external call, not a
port write), so no final-state observable exists — ``NOT_TRANSFERABLE`` by definition, reported
as such, never silently dropped.
"""

from __future__ import annotations

import itertools
from uuid import UUID

from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.base.exceptions.model import CoreException, ExceptionKind
from forze.testing import Conductor, Gate
from forze_dst.conformance import ConformanceBackend, is_serialization_conflict
from forze_dst.conformance.transfer import Detection, TransferScript

from .activation import (
    PROFILE_SPEC,
    SERVE_SPEC,
    ProfileCreate,
    ProfileUpdate,
    ServeLogCreate,
)
from .dlock import (
    BALANCE_SPEC,
    LOCK_SPEC,
    TRANSFER_SPEC,
    BalanceCreate,
    BalanceUpdate,
    LockRowCreate,
    TransferRowCreate,
)
from .idempotency import (
    ACK_SPEC,
    CHARGE_SPEC,
    EFFECT_SPEC,
    RECEIPT_SPEC,
    SUBMISSION_SPEC,
    AckRowCreate,
    ChargeRowCreate,
    EffectRowCreate,
    ReceiptRowCreate,
    SubmissionRowCreate,
)
from .messaging import (
    HANDLED_SPEC,
    INBOX_SPEC,
    OUTBOX_EVENT_SPEC,
    SHIPMENT_SPEC,
    HandledRowCreate,
    InboxRowCreate,
    OutboxEventCreate,
    ShipmentCreate,
)
from .tenancy import (
    CACHE_SPEC,
    RW_LOG_SPEC,
    SOURCE_SPEC,
    TENANT_ROW_SPEC,
    CacheRowCreate,
    CacheRowUpdate,
    SourceCreate,
    SourceUpdate,
    TenantRowCreate,
)
from .transactions import (
    ONCALL_SPEC,
    ORDER_SPEC,
    PAYMENT_SPEC,
    RESERVATION_SPEC,
    OnCallSlotCreate,
    OnCallSlotUpdate,
    OrderCreate,
    OrderUpdate,
    PaymentCreate,
    ReservationCreate,
)

# ----------------------- #

# Fresh identifiers per script invocation — self-isolation on a reused (real) store. High offsets
# keep them clear of the corpus simulations' fixed ids.
_order_seq = itertools.count(1000)
_guest_seq = itertools.count(2000)
_command_seq = itertools.count(3000)
_message_seq = itertools.count(4000)
_shipment_seq = itertools.count(6000)
_ack_seq = itertools.count(7000)
_resource_seq = itertools.count(8000)
_tenant_seq = itertools.count(9000)
_cache_seq = itertools.count(20000)


def _swallow_conflict(error: CoreException) -> None:
    """Re-raise anything that is not "the duplicate/stale write was rejected"."""

    if error.kind is not ExceptionKind.CONFLICT and not is_serialization_conflict(error):
        raise error


# ....................... #
# T family — two payers race one order; both read the unpaid order before either writes.


async def _run_payment_race(
    backend: ConformanceBackend,
    *,
    guarded: bool,
    row_first: bool,
    tx: bool,
) -> Detection:
    sessions = backend.contexts(2)
    a_ctx, b_ctx = sessions[0], sessions[1]
    scope = backend.scope_name
    order_id = UUID(int=next(_order_seq))

    async with a_ctx.tx_ctx.scope(scope):
        await a_ctx.document.command(ORDER_SPEC).create(OrderCreate(), id=order_id)

    async def transition(ctx: ExecutionContext, rev: int) -> None:
        if guarded:
            await ctx.document.command(ORDER_SPEC).update(order_id, rev, OrderUpdate(paid=True))
        else:
            # MUTANT (T1): the blind bulk write — every payer "wins".
            await ctx.document.command(ORDER_SPEC).update_matching(
                {"$values": {"id": {"$eq": order_id}}}, OrderUpdate(paid=True), return_new=False
            )

    def pay(ctx: ExecutionContext):  # type: ignore[no-untyped-def]
        async def session(gate: Gate) -> None:
            if tx:
                try:
                    async with ctx.tx_ctx.scope(scope):
                        order = await ctx.document.query(ORDER_SPEC).get(order_id)
                        await gate.checkpoint()  # both read unpaid before either writes
                        if order.paid:
                            return
                        if row_first:
                            await ctx.document.command(PAYMENT_SPEC).create(
                                PaymentCreate(order_id=order_id)
                            )
                            await transition(ctx, order.rev)
                        else:
                            await transition(ctx, order.rev)
                            await ctx.document.command(PAYMENT_SPEC).create(
                                PaymentCreate(order_id=order_id)
                            )
                except CoreException as error:
                    _swallow_conflict(error)  # the loser's rejected transition, not a bug
            else:
                # MUTANT (T3): the transaction boundary removed — the charge row and the
                # guarded transition commit independently, so the loser's row survives.
                async with ctx.tx_ctx.scope(scope):
                    order = await ctx.document.query(ORDER_SPEC).get(order_id)
                await gate.checkpoint()  # both read unpaid before either proceeds
                if order.paid:
                    return
                async with ctx.tx_ctx.scope(scope):
                    await ctx.document.command(PAYMENT_SPEC).create(
                        PaymentCreate(order_id=order_id)
                    )
                try:
                    async with ctx.tx_ctx.scope(scope):
                        await transition(ctx, order.rev)
                except CoreException as error:
                    _swallow_conflict(error)

        return session

    await Conductor(schedule=("A", "B")).run({"A": pay(a_ctx), "B": pay(b_ctx)})

    async with a_ctx.tx_ctx.scope(scope):
        total = await a_ctx.document.query(PAYMENT_SPEC).count(
            {"$values": {"order_id": order_id}}
        )

    return Detection.DETECTED if total > 1 else Detection.CLEAN


# ....................... #
# T5 / its control — two reservers race one guest.


async def _run_reservation_race(backend: ConformanceBackend, *, unique_key: bool) -> Detection:
    sessions = backend.contexts(2)
    a_ctx, b_ctx = sessions[0], sessions[1]
    scope = backend.scope_name
    guest = next(_guest_seq)

    def reserve(ctx: ExecutionContext):  # type: ignore[no-untyped-def]
        async def session(gate: Gate) -> None:
            try:
                async with ctx.tx_ctx.scope(scope):
                    if unique_key:
                        await gate.checkpoint()  # park before the insert, mirroring the mutant
                        await ctx.document.command(RESERVATION_SPEC).create(
                            ReservationCreate(guest=guest), id=UUID(int=guest)
                        )
                    else:
                        # MUTANT (T5): check-then-act — both reservers see zero and both insert.
                        count = await ctx.document.query(RESERVATION_SPEC).count(
                            {"$values": {"guest": guest}}
                        )
                        await gate.checkpoint()  # both checked before either inserts
                        if count == 0:
                            await ctx.document.command(RESERVATION_SPEC).create(
                                ReservationCreate(guest=guest)
                            )
            except CoreException as error:
                _swallow_conflict(error)  # the duplicate was rejected — not merged

        return session

    await Conductor(schedule=("A", "B")).run({"A": reserve(a_ctx), "B": reserve(b_ctx)})

    async with a_ctx.tx_ctx.scope(scope):
        total = await a_ctx.document.query(RESERVATION_SPEC).count({"$values": {"guest": guest}})

    return Detection.DETECTED if total > 1 else Detection.CLEAN


# ....................... #
# I family — the same command delivered twice (a client retry); plain re-invocation.


async def _run_command_retry(backend: ConformanceBackend, *, keyed: bool) -> Detection:
    ctx = backend.contexts(1)[0]
    scope = backend.scope_name
    command = next(_command_seq)

    for _delivery in range(2):
        try:
            async with ctx.tx_ctx.scope(scope):
                if keyed:
                    await ctx.document.command(CHARGE_SPEC).create(
                        ChargeRowCreate(command=command), id=UUID(int=command)
                    )
                else:
                    # MUTANT (I1): a fresh charge row per delivery — the retry charges twice.
                    await ctx.document.command(CHARGE_SPEC).create(
                        ChargeRowCreate(command=command)
                    )
        except CoreException as error:
            _swallow_conflict(error)  # already charged — the keyed retry is a no-op

    async with ctx.tx_ctx.scope(scope):
        total = await ctx.document.query(CHARGE_SPEC).count({"$values": {"command": command}})

    return Detection.DETECTED if total > 1 else Detection.CLEAN


# ....................... #
# M family — the same message delivered twice (broker redelivery); plain re-delivery.


async def _run_redelivery(backend: ConformanceBackend, *, inbox: bool) -> Detection:
    ctx = backend.contexts(1)[0]
    scope = backend.scope_name
    message = next(_message_seq)

    for _delivery in range(2):
        try:
            async with ctx.tx_ctx.scope(scope):
                if inbox:
                    # Inbox-first, effect in the same transaction — the redelivery conflicts.
                    await ctx.document.command(INBOX_SPEC).create(
                        InboxRowCreate(message=message), id=UUID(int=message)
                    )
                await ctx.document.command(HANDLED_SPEC).create(
                    HandledRowCreate(message=message)
                )
        except CoreException as error:
            _swallow_conflict(error)  # already processed — the redelivery is a no-op

    async with ctx.tx_ctx.scope(scope):
        total = await ctx.document.query(HANDLED_SPEC).count({"$values": {"message": message}})

    return Detection.DETECTED if total > 1 else Detection.CLEAN


# ....................... #
# D family — the lease-row lock protocol; both critical sections forced to read before either
# writes (the same two-checkpoint weave the corpus workload explores).


async def _run_lock_race(backend: ConformanceBackend, *, mode: str) -> Detection:
    sessions = backend.contexts(2)
    a_ctx, b_ctx = sessions[0], sessions[1]
    scope = backend.scope_name
    resource = next(_resource_seq)
    balance_id = UUID(int=100000 + resource)
    lock_id = UUID(int=200000 + resource)

    async with a_ctx.tx_ctx.scope(scope):
        await a_ctx.document.command(BALANCE_SPEC).create(
            BalanceCreate(resource=resource, value=0), id=balance_id
        )

    def transfer(ctx: ExecutionContext):  # type: ignore[no-untyped-def]
        async def session(gate: Gate) -> None:
            await gate.checkpoint()  # start gate — the schedule fully orders both sessions
            if mode == "locked":
                try:
                    async with ctx.tx_ctx.scope(scope):
                        await ctx.document.command(LOCK_SPEC).create(
                            LockRowCreate(resource=resource), id=lock_id
                        )
                except CoreException as error:
                    _swallow_conflict(error)
                    return  # the lease is held — back off
            elif mode == "nonatomic":
                # MUTANT (D3): check-then-set — both see it free before either creates.
                async with ctx.tx_ctx.scope(scope):
                    held = await ctx.document.query(LOCK_SPEC).count(
                        {"$values": {"resource": resource}}
                    )
                await gate.checkpoint()  # both checked before either creates
                if held:
                    return
                async with ctx.tx_ctx.scope(scope):
                    await ctx.document.command(LOCK_SPEC).create(LockRowCreate(resource=resource))
            # MUTANT (D1): mode == "skip" — straight into the critical section.

            async with ctx.tx_ctx.scope(scope):
                balance = await ctx.document.query(BALANCE_SPEC).get(balance_id)
            await gate.checkpoint()  # both read before either writes
            async with ctx.tx_ctx.scope(scope):
                await ctx.document.command(BALANCE_SPEC).update_matching(
                    {"$values": {"id": {"$eq": balance_id}}},
                    BalanceUpdate(value=balance.value + 1),
                    return_new=False,
                )
                await ctx.document.command(TRANSFER_SPEC).create(
                    TransferRowCreate(resource=resource)
                )

        return session

    schedule = ("A", "B") * (3 if mode == "nonatomic" else 2)
    await Conductor(schedule=schedule).run({"A": transfer(a_ctx), "B": transfer(b_ctx)})

    async with a_ctx.tx_ctx.scope(scope):
        value = (await a_ctx.document.query(BALANCE_SPEC).get(balance_id)).value
        transfers = await a_ctx.document.query(TRANSFER_SPEC).count(
            {"$values": {"resource": resource}}
        )

    return Detection.DETECTED if value != transfers else Detection.CLEAN


# ....................... #
# N family — sequential provocations: the leak and the stale read-through need no interleaving.


async def _run_tenant_browse(backend: ConformanceBackend, *, filtered: bool) -> Detection:
    ctx = backend.contexts(1)[0]
    scope = backend.scope_name
    owner, viewer = next(_tenant_seq), next(_tenant_seq)

    async with ctx.tx_ctx.scope(scope):
        await ctx.document.command(TENANT_ROW_SPEC).create(TenantRowCreate(tenant=owner))

    async with ctx.tx_ctx.scope(scope):
        if filtered:
            seen = await ctx.document.query(TENANT_ROW_SPEC).count(
                {"$values": {"tenant": viewer}}
            )
        else:
            # MUTANT (N1): the tenant predicate is gone — every tenant's rows are visible.
            seen = await ctx.document.query(TENANT_ROW_SPEC).count()

    return Detection.DETECTED if seen > 0 else Detection.CLEAN


async def _run_stale_cache(backend: ConformanceBackend, *, invalidate: bool) -> Detection:
    ctx = backend.contexts(1)[0]
    scope = backend.scope_name
    source_id = UUID(int=next(_cache_seq))
    cache_id = UUID(int=next(_cache_seq))

    async with ctx.tx_ctx.scope(scope):
        await ctx.document.command(SOURCE_SPEC).create(SourceCreate(version=0), id=source_id)
        await ctx.document.command(CACHE_SPEC).create(CacheRowCreate(version=0), id=cache_id)

    async with ctx.tx_ctx.scope(scope):
        source = await ctx.document.query(SOURCE_SPEC).get(source_id)
        written = source.version + 1
        await ctx.document.command(SOURCE_SPEC).update(
            source_id, source.rev, SourceUpdate(version=written)
        )
        if invalidate:
            cache = await ctx.document.query(CACHE_SPEC).get(cache_id)
            await ctx.document.command(CACHE_SPEC).update(
                cache_id, cache.rev, CacheRowUpdate(version=written)
            )
        # MUTANT (N2): the write path never touches the cache.

    async with ctx.tx_ctx.scope(scope):
        cached = await ctx.document.query(CACHE_SPEC).get(cache_id)

    return Detection.DETECTED if cached.version < written else Detection.CLEAN


async def _d1(backend: ConformanceBackend) -> Detection:
    return await _run_lock_race(backend, mode="skip")


async def _d3(backend: ConformanceBackend) -> Detection:
    return await _run_lock_race(backend, mode="nonatomic")


async def _ctrl_lock_protocol(backend: ConformanceBackend) -> Detection:
    return await _run_lock_race(backend, mode="locked")


async def _n1(backend: ConformanceBackend) -> Detection:
    return await _run_tenant_browse(backend, filtered=False)


async def _ctrl_tenant_filtered_browse(backend: ConformanceBackend) -> Detection:
    return await _run_tenant_browse(backend, filtered=True)


async def _n2(backend: ConformanceBackend) -> Detection:
    return await _run_stale_cache(backend, invalidate=False)


async def _ctrl_cache_invalidate_in_tx(backend: ConformanceBackend) -> Detection:
    return await _run_stale_cache(backend, invalidate=True)


# ....................... #
# T3 deep instance — torn activation: the reader is forced between the two provision commits.

_profile_seq = itertools.count(5000)


async def _run_torn_activation(backend: ConformanceBackend, *, atomic: bool) -> Detection:
    sessions = backend.contexts(2)
    provisioner, reader = sessions[0], sessions[1]
    scope = backend.scope_name
    profile_id = UUID(int=next(_profile_seq))

    async def provision(gate: Gate) -> None:
        if atomic:
            await gate.checkpoint()  # park at start so the schedule fully orders both sessions
            async with provisioner.tx_ctx.scope(scope):
                await provisioner.document.command(PROFILE_SPEC).create(
                    ProfileCreate(ready=False), id=profile_id
                )
                profile = await provisioner.document.query(PROFILE_SPEC).get(profile_id)
                await provisioner.document.command(PROFILE_SPEC).update(
                    profile_id, profile.rev, ProfileUpdate(ready=True)
                )
            return

        # MUTANT (T3, deep): create commits alone; park inside the torn window; activate after.
        async with provisioner.tx_ctx.scope(scope):
            await provisioner.document.command(PROFILE_SPEC).create(
                ProfileCreate(ready=False), id=profile_id
            )
        await gate.checkpoint()  # the torn window is open — created, not yet ready
        async with provisioner.tx_ctx.scope(scope):
            profile = await provisioner.document.query(PROFILE_SPEC).get(profile_id)
            await provisioner.document.command(PROFILE_SPEC).update(
                profile_id, profile.rev, ProfileUpdate(ready=True)
            )

    async def serve(gate: Gate) -> None:
        await gate.checkpoint()  # released inside the window (mutant) / after commit (control)
        async with reader.tx_ctx.scope(scope):
            profile = await reader.document.query(PROFILE_SPEC).get(profile_id)
            await reader.document.command(SERVE_SPEC).create(
                ServeLogCreate(profile=profile_id, state="ready" if profile.ready else "torn")
            )

    schedule = ("provision", "reader") if atomic else ("reader", "provision")
    await Conductor(schedule=schedule).run({"provision": provision, "reader": serve})

    async with provisioner.tx_ctx.scope(scope):
        torn = await provisioner.document.query(SERVE_SPEC).count(
            {"$values": {"profile": profile_id, "state": "torn"}}
        )

    return Detection.DETECTED if torn > 0 else Detection.CLEAN


# ....................... #
# Crash-fault analogs (FAULT_ANALOG tier): the simulated crash between two commits maps onto a
# real backend as the session ABANDONING after the first commit (a died process holds no further
# writes); the atomic control's "crash" analog is an abort raised inside the single transaction.


class _SimulatedDeath(Exception):
    """The analog crash point — raised inside the atomic control's transaction to abort it."""


async def _run_dual_write_crash(backend: ConformanceBackend, *, atomic: bool) -> Detection:
    ctx = backend.contexts(1)[0]
    scope = backend.scope_name
    ref = next(_shipment_seq)

    if atomic:
        # Control analog: the crash lands inside the single transaction — both writes abort.
        try:
            async with ctx.tx_ctx.scope(scope):
                await ctx.document.command(SHIPMENT_SPEC).create(ShipmentCreate(ref=ref))
                await ctx.document.command(OUTBOX_EVENT_SPEC).create(OutboxEventCreate(ref=ref))
                raise _SimulatedDeath()
        except _SimulatedDeath:
            pass
    else:
        # MUTANT analog (M1): the process dies after the state commit — the event never leaves.
        async with ctx.tx_ctx.scope(scope):
            await ctx.document.command(SHIPMENT_SPEC).create(ShipmentCreate(ref=ref))
        # (death: the second transaction never runs)

    async with ctx.tx_ctx.scope(scope):
        shipments = await ctx.document.query(SHIPMENT_SPEC).count({"$values": {"ref": ref}})
        events = await ctx.document.query(OUTBOX_EVENT_SPEC).count({"$values": {"ref": ref}})

    return Detection.DETECTED if shipments != events else Detection.CLEAN


async def _run_ack_crash(backend: ConformanceBackend, *, ack_first: bool) -> Detection:
    ctx = backend.contexts(1)[0]
    scope = backend.scope_name
    message = next(_ack_seq)

    if ack_first:
        # MUTANT analog (I3): ack commits, the process dies, the redelivery sees the ack and skips.
        async with ctx.tx_ctx.scope(scope):
            await ctx.document.command(ACK_SPEC).create(
                AckRowCreate(message=message), id=UUID(int=message)
            )
        # (death; then the redelivery:)
        try:
            async with ctx.tx_ctx.scope(scope):
                await ctx.document.command(ACK_SPEC).create(
                    AckRowCreate(message=message), id=UUID(int=message)
                )
                await ctx.document.command(EFFECT_SPEC).create(EffectRowCreate(message=message))
        except CoreException as error:
            _swallow_conflict(error)  # "already done" — the loss is now permanent
    else:
        # Control analog: effect+ack in one transaction; the crash aborts both; the redelivery
        # completes the work.
        try:
            async with ctx.tx_ctx.scope(scope):
                await ctx.document.command(ACK_SPEC).create(
                    AckRowCreate(message=message), id=UUID(int=message)
                )
                await ctx.document.command(EFFECT_SPEC).create(EffectRowCreate(message=message))
                raise _SimulatedDeath()
        except _SimulatedDeath:
            pass
        try:
            async with ctx.tx_ctx.scope(scope):
                await ctx.document.command(ACK_SPEC).create(
                    AckRowCreate(message=message), id=UUID(int=message)
                )
                await ctx.document.command(EFFECT_SPEC).create(EffectRowCreate(message=message))
        except CoreException as error:
            _swallow_conflict(error)

    async with ctx.tx_ctx.scope(scope):
        acked = await ctx.document.query(ACK_SPEC).count({"$values": {"message": message}})
        effects = await ctx.document.query(EFFECT_SPEC).count({"$values": {"message": message}})

    return Detection.DETECTED if acked > 0 and effects == 0 else Detection.CLEAN


async def _m1(backend: ConformanceBackend) -> Detection:
    return await _run_dual_write_crash(backend, atomic=False)


async def _ctrl_outbox_in_tx(backend: ConformanceBackend) -> Detection:
    return await _run_dual_write_crash(backend, atomic=True)


async def _i3(backend: ConformanceBackend) -> Detection:
    return await _run_ack_crash(backend, ack_first=True)


async def _ctrl_process_then_ack(backend: ConformanceBackend) -> Detection:
    return await _run_ack_crash(backend, ack_first=False)



# ....................... #


# ....................... #
# I2 — the naive in-handler retry loop; a plain sequential duplicate submission provokes it
# (the ack conflict fires without any interleaving, exactly like the corpus workload).

_i2_order_seq = itertools.count(23000)


async def _run_naive_retry(backend: ConformanceBackend, *, keyed: bool) -> Detection:
    ctx = backend.contexts(1)[0]
    scope = backend.scope_name
    order = next(_i2_order_seq)
    commands = (next(_command_seq), next(_command_seq))

    for command in commands:  # two submitters for the same order — the second one loses
        for _attempt in range(2):
            if keyed:
                try:
                    async with ctx.tx_ctx.scope(scope):
                        await ctx.document.command(RECEIPT_SPEC).create(
                            ReceiptRowCreate(command=command), id=UUID(int=700000 + command)
                        )
                except CoreException as error:
                    _swallow_conflict(error)  # already receipted — the re-run is a no-op
            else:
                # MUTANT (I2): a fresh receipt per attempt, committed before the ack.
                async with ctx.tx_ctx.scope(scope):
                    await ctx.document.command(RECEIPT_SPEC).create(
                        ReceiptRowCreate(command=command)
                    )

            try:
                async with ctx.tx_ctx.scope(scope):
                    await ctx.document.command(SUBMISSION_SPEC).create(
                        SubmissionRowCreate(booking=order), id=UUID(int=800000 + order)
                    )
                break
            except CoreException as error:
                _swallow_conflict(error)  # naive retry: re-run the WHOLE effect block

    async with ctx.tx_ctx.scope(scope):
        worst = 0
        for command in commands:
            total = await ctx.document.query(RECEIPT_SPEC).count(
                {"$values": {"command": command}}
            )
            worst = max(worst, total)

    return Detection.DETECTED if worst > 1 else Detection.CLEAN


# ....................... #
# T4 — write skew at the declared level: the battery's on-call weave (both read before either
# commits), run at SNAPSHOT (mutant) vs SERIALIZABLE (control) — the level is the only knob.

_rota_seq = itertools.count(24000)


async def _run_weakened_oncall(backend: ConformanceBackend, *, serializable: bool) -> Detection:
    sessions = backend.contexts(2)
    a_ctx, b_ctx = sessions[0], sessions[1]
    scope = backend.scope_name
    rota = next(_rota_seq)
    slots = {doctor: UUID(int=900000 + rota * 2 + doctor) for doctor in (0, 1)}
    level = IsolationLevel.SERIALIZABLE if serializable else IsolationLevel.SNAPSHOT

    async with a_ctx.tx_ctx.scope(scope):
        command = a_ctx.document.command(ONCALL_SPEC)
        for doctor, slot_id in slots.items():
            await command.create(
                OnCallSlotCreate(rota=rota, doctor=doctor, on_call=True), id=slot_id
            )

    def go_off(ctx: ExecutionContext, doctor: int):  # type: ignore[no-untyped-def]
        async def session(gate: Gate) -> None:
            try:
                async with ctx.tx_ctx.scope(scope, isolation=level):
                    query = ctx.document.query(ONCALL_SPEC)
                    mine = await query.get(slots[doctor])
                    other = await query.get(slots[1 - doctor])
                    await gate.checkpoint()  # both sessions have read before either writes

                    if mine.on_call and other.on_call:
                        await ctx.document.command(ONCALL_SPEC).update(
                            mine.id, mine.rev, OnCallSlotUpdate(on_call=False)
                        )
                    await gate.checkpoint()  # commit happens on scope exit, after this
            except CoreException as error:
                if not is_serialization_conflict(error):
                    raise  # the serialization abort is the control's back-off path

        return session

    await Conductor(schedule=("A", "A", "B", "B")).run(
        {"A": go_off(a_ctx, 0), "B": go_off(b_ctx, 1)}
    )

    async with a_ctx.tx_ctx.scope(scope):
        covered = await a_ctx.document.query(ONCALL_SPEC).count(
            {"$values": {"rota": rota, "on_call": True}}
        )

    return Detection.DETECTED if covered == 0 else Detection.CLEAN


# ....................... #
# D2 — the release placement race: the holder lets go of the lease inside the critical section
# (mutant) or after the write (control); a spinning waiter probes the lock at every checkpoint.


async def _run_release_race(backend: ConformanceBackend, *, early: bool) -> Detection:
    sessions = backend.contexts(2)
    a_ctx, b_ctx = sessions[0], sessions[1]
    scope = backend.scope_name
    resource = next(_resource_seq)
    balance_id = UUID(int=100000 + resource)
    lock_id = UUID(int=200000 + resource)

    async with a_ctx.tx_ctx.scope(scope):
        await a_ctx.document.command(BALANCE_SPEC).create(
            BalanceCreate(resource=resource, value=0), id=balance_id
        )

    async def write_transfer(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope(scope):
            balance = await ctx.document.query(BALANCE_SPEC).get(balance_id)
        async with ctx.tx_ctx.scope(scope):
            await ctx.document.command(BALANCE_SPEC).update_matching(
                {"$values": {"id": {"$eq": balance_id}}},
                BalanceUpdate(value=balance.value + 1),
                return_new=False,
            )
            await ctx.document.command(TRANSFER_SPEC).create(TransferRowCreate(resource=resource))

    async def holder(gate: Gate) -> None:
        await gate.checkpoint()  # start gate — the schedule fully orders both sessions
        async with a_ctx.tx_ctx.scope(scope):
            lease = await a_ctx.document.command(LOCK_SPEC).create(
                LockRowCreate(resource=resource), id=lock_id
            )
        async with a_ctx.tx_ctx.scope(scope):
            balance = await a_ctx.document.query(BALANCE_SPEC).get(balance_id)
        await gate.checkpoint()  # inside the critical section
        if early:
            # MUTANT (D2): the lease is dropped before the write lands.
            async with a_ctx.tx_ctx.scope(scope):
                await a_ctx.document.command(LOCK_SPEC).kill(lease.id)
        await gate.checkpoint()  # the (mutant's) window is open
        async with a_ctx.tx_ctx.scope(scope):
            await a_ctx.document.command(BALANCE_SPEC).update_matching(
                {"$values": {"id": {"$eq": balance_id}}},
                BalanceUpdate(value=balance.value + 1),
                return_new=False,
            )
            await a_ctx.document.command(TRANSFER_SPEC).create(
                TransferRowCreate(resource=resource)
            )
        if not early:
            async with a_ctx.tx_ctx.scope(scope):
                await a_ctx.document.command(LOCK_SPEC).kill(lease.id)

    async def waiter(gate: Gate) -> None:
        await gate.checkpoint()  # start gate
        for _attempt in range(3):
            try:
                async with b_ctx.tx_ctx.scope(scope):
                    await b_ctx.document.command(LOCK_SPEC).create(
                        LockRowCreate(resource=resource), id=lock_id
                    )
                break
            except CoreException as error:
                _swallow_conflict(error)
                await gate.checkpoint()  # the lease is held — spin
        else:
            return  # never acquired — back off
        await write_transfer(b_ctx)

    # Mutant: the waiter's 2nd turn acquires inside the window and runs to completion (2 B
    # slots); control: the lease only frees after A's 3rd turn, so the waiter needs a 3rd slot.
    schedule = ("A", "B", "A", "B", "A") if early else ("A", "B", "A", "B", "A", "B")
    await Conductor(schedule=schedule).run({"A": holder, "B": waiter})

    async with a_ctx.tx_ctx.scope(scope):
        value = (await a_ctx.document.query(BALANCE_SPEC).get(balance_id)).value
        transfers = await a_ctx.document.query(TRANSFER_SPEC).count(
            {"$values": {"resource": resource}}
        )

    return Detection.DETECTED if value != transfers else Detection.CLEAN


async def _t1(backend: ConformanceBackend) -> Detection:
    return await _run_payment_race(backend, guarded=False, row_first=False, tx=True)


async def _t3(backend: ConformanceBackend) -> Detection:
    return await _run_payment_race(backend, guarded=True, row_first=True, tx=False)


async def _t5(backend: ConformanceBackend) -> Detection:
    return await _run_reservation_race(backend, unique_key=False)


async def _i1(backend: ConformanceBackend) -> Detection:
    return await _run_command_retry(backend, keyed=False)


async def _i2(backend: ConformanceBackend) -> Detection:
    return await _run_naive_retry(backend, keyed=False)


async def _ctrl_idempotent_retry(backend: ConformanceBackend) -> Detection:
    return await _run_naive_retry(backend, keyed=True)


async def _t4(backend: ConformanceBackend) -> Detection:
    return await _run_weakened_oncall(backend, serializable=False)


async def _ctrl_serializable_oncall(backend: ConformanceBackend) -> Detection:
    return await _run_weakened_oncall(backend, serializable=True)


async def _d2(backend: ConformanceBackend) -> Detection:
    return await _run_release_race(backend, early=True)


async def _ctrl_release_after_write(backend: ConformanceBackend) -> Detection:
    return await _run_release_race(backend, early=False)


async def _m2(backend: ConformanceBackend) -> Detection:
    return await _run_redelivery(backend, inbox=False)


async def _t3_torn(backend: ConformanceBackend) -> Detection:
    return await _run_torn_activation(backend, atomic=False)


async def _ctrl_atomic_provision(backend: ConformanceBackend) -> Detection:
    return await _run_torn_activation(backend, atomic=True)


async def _ctrl_row_after_guard(backend: ConformanceBackend) -> Detection:
    return await _run_payment_race(backend, guarded=True, row_first=False, tx=True)


async def _ctrl_row_before_guard_in_tx(backend: ConformanceBackend) -> Detection:
    return await _run_payment_race(backend, guarded=True, row_first=True, tx=True)


async def _ctrl_unique_reservation(backend: ConformanceBackend) -> Detection:
    return await _run_reservation_race(backend, unique_key=True)


async def _ctrl_retry_with_key(backend: ConformanceBackend) -> Detection:
    return await _run_command_retry(backend, keyed=True)


async def _ctrl_inbox_consumer(backend: ConformanceBackend) -> Detection:
    return await _run_redelivery(backend, inbox=True)


SCRIPTS: tuple[TransferScript, ...] = (
    TransferScript(mutant_id="T1-blind-write-payment", expect_detected=True, run=_t1),
    TransferScript(mutant_id="T3-payment-outside-tx", expect_detected=True, run=_t3),
    TransferScript(mutant_id="T3-torn-activation", expect_detected=True, run=_t3_torn),
    TransferScript(mutant_id="T5-unchecked-reservation", expect_detected=True, run=_t5),
    TransferScript(mutant_id="T4-weakened-oncall", expect_detected=True, run=_t4),
    TransferScript(mutant_id="I1-retry-without-key", expect_detected=True, run=_i1),
    TransferScript(mutant_id="I2-naive-retry-loop", expect_detected=True, run=_i2),
    TransferScript(mutant_id="D2-early-lease-release", expect_detected=True, run=_d2),
    TransferScript(mutant_id="M2-consumer-without-inbox", expect_detected=True, run=_m2),
    TransferScript(mutant_id="M1-dual-write-shipment", expect_detected=True, run=_m1),
    TransferScript(mutant_id="I3-ack-before-processing", expect_detected=True, run=_i3),
    TransferScript(mutant_id="D1-skip-lock", expect_detected=True, run=_d1),
    TransferScript(mutant_id="D3-nonatomic-acquire", expect_detected=True, run=_d3),
    TransferScript(mutant_id="N1-drop-tenant-predicate", expect_detected=True, run=_n1),
    TransferScript(mutant_id="N2-stale-cache", expect_detected=True, run=_n2),
    TransferScript(mutant_id="ctrl-row-after-guard", expect_detected=False, run=_ctrl_row_after_guard),
    TransferScript(
        mutant_id="ctrl-row-before-guard-in-tx",
        expect_detected=False,
        run=_ctrl_row_before_guard_in_tx,
    ),
    TransferScript(
        mutant_id="ctrl-unique-reservation", expect_detected=False, run=_ctrl_unique_reservation
    ),
    TransferScript(
        mutant_id="ctrl-atomic-provision", expect_detected=False, run=_ctrl_atomic_provision
    ),
    TransferScript(mutant_id="ctrl-retry-with-key", expect_detected=False, run=_ctrl_retry_with_key),
    TransferScript(
        mutant_id="ctrl-idempotent-retry", expect_detected=False, run=_ctrl_idempotent_retry
    ),
    TransferScript(
        mutant_id="ctrl-serializable-oncall", expect_detected=False, run=_ctrl_serializable_oncall
    ),
    TransferScript(
        mutant_id="ctrl-release-after-write", expect_detected=False, run=_ctrl_release_after_write
    ),
    TransferScript(mutant_id="ctrl-inbox-consumer", expect_detected=False, run=_ctrl_inbox_consumer),
    TransferScript(mutant_id="ctrl-outbox-in-tx", expect_detected=False, run=_ctrl_outbox_in_tx),
    TransferScript(
        mutant_id="ctrl-process-then-ack", expect_detected=False, run=_ctrl_process_then_ack
    ),
    TransferScript(mutant_id="ctrl-lock-protocol", expect_detected=False, run=_ctrl_lock_protocol),
    TransferScript(
        mutant_id="ctrl-tenant-filtered-browse",
        expect_detected=False,
        run=_ctrl_tenant_filtered_browse,
    ),
    TransferScript(
        mutant_id="ctrl-cache-invalidate-in-tx",
        expect_detected=False,
        run=_ctrl_cache_invalidate_in_tx,
    ),
)
"""Every transferable corpus instance (all mutants + all controls); T2 is `NOT_TRANSFERABLE`."""
