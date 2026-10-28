"""Durable-run store contract: the run/instance record backing crash recovery.

A durable run is one invocation of a registered durable function (or saga). The store
persists the run instance so a crashed run can be re-claimed and resumed — the step-memo
journal (:class:`DurableFunctionStepPort`) then replays completed steps and the first
incomplete step runs live. Backend-agnostic: implemented over Postgres (self-hosted) and
an in-memory mock (tests / simulation).
"""

from __future__ import annotations

from collections.abc import Awaitable, Sequence
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Protocol, final, runtime_checkable
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


def check_enqueue_keys(idempotency_key: str | None, coalesce_key: str | None) -> None:
    """Refuse an enqueue naming both keys: one converges forever, the other only while the
    run waits unclaimed, and a run cannot honour both.

    :raises CoreException: ``validation`` when both are set.
    """

    if idempotency_key is not None and coalesce_key is not None:
        raise exc.validation(
            "A durable run takes an idempotency_key or a coalesce_key, not both.",
        )


# ....................... #


class DurableRunStatus(StrEnum):
    """Lifecycle state of a durable run."""

    PENDING = "pending"
    """Enqueued, not yet claimed for execution — or back from a failed attempt, waiting for
    its retry (:meth:`DurableRunStorePort.mark_retry`), or requeued by an operator."""

    RUNNING = "running"
    """Claimed and executing (leased); a crash leaves it here for the recovery scanner."""

    COMPLETED = "completed"
    """Finished successfully; :attr:`DurableRunRecord.output_json` holds the result."""

    FAILED = "failed"
    """Finished with an error before any point of no return."""

    FORWARD_INCOMPLETE = "forward_incomplete"
    """A saga committed at its pivot but could not complete forward (manual intervention)."""

    CANCELLED = "cancelled"
    """An operator asked the run to stop and it did. Deliberately distinct from ``FAILED``:
    nothing is wrong with the code, so a dashboard must not page anyone for it."""

    TIMED_OUT = "timed_out"
    """The run outlived the runner's ``max_run_duration`` cap and its body was cancelled.

    Distinct from ``FAILED`` because the two demand different responses: ``FAILED`` sends you
    to the body's code, ``TIMED_OUT`` sends you to the cap, the workload's size, or a hung
    peer. :attr:`DurableRunRecord.error` still carries the human-readable deadline reason."""

    QUARANTINED = "quarantined"
    """Recovery could not decrypt the run for long enough, its tenant's key being gone, and
    stopped trying. Not terminal: the run still owes its work, but the recovery scan never
    claims it, so however many there are they cannot crowd the runs behind them.

    :meth:`~forze.application.contracts.durable.function.DurableRunAdminPort.requeue_quarantined`
    puts it back to ``PENDING`` once the key is back; until then a listing shows it with
    :attr:`DurableRunRecord.payloads_unreadable` set."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableLeaseRenewal:
    """The verdict of one heartbeat renewal, plus what the store saw while answering it.

    A heartbeat is the one round-trip a running body already makes on a fixed cadence, so the
    cancel stamp rides back on it rather than costing a second polling loop. Observation
    latency for a cancel request is therefore one heartbeat interval
    (``lease_for / heartbeat_divisor``).
    """

    held: bool
    """Whether the lease was extended — i.e. whether this worker is still the claim holder.

    ``False`` means another worker reclaimed the run (a newer claim advanced ``attempts``),
    so the caller must stop before its body double-executes the new owner's work."""

    cancel_requested: bool = False
    """Whether an operator has asked this run to stop (see
    :meth:`~forze.application.contracts.durable.function.DurableRunAdminPort.request_cancel`).

    Advisory and unfenced — anyone may *ask*. Only the fence-holding runner may land
    ``CANCELLED``, so a stale worker reading this cannot cancel a run out from under the
    new owner. Always ``False`` when :attr:`held` is ``False`` (a store that no longer
    recognises the caller as holder reports no state about the run)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableRunRecord:
    """A persisted durable-run instance."""

    run_id: str
    """Unique run identifier (a uuid7 string)."""

    name: str
    """Registered function/saga name this run executes."""

    status: DurableRunStatus
    """Current lifecycle state."""

    idempotency_key: str | None = None
    """Optional key deduplicating re-submits to a single logical run."""

    input_json: JsonDict | None = None
    """Encoded invocation arguments (a keyring seals them at rest when configured)."""

    output_json: JsonDict | None = None
    """Encoded result, present once :attr:`status` is ``COMPLETED``."""

    error: str | None = None
    """Failure message, present once :attr:`status` is
    ``FAILED``/``FORWARD_INCOMPLETE``/``TIMED_OUT``, and on a ``PENDING`` run waiting for a
    retry or requeued after a failure, where it is the failure that sent it back.

    Also populated on some ``CANCELLED`` runs, where it is an explanatory **note** rather
    than a failure — a cancelled saga records which step it stopped at and that its completed
    steps were compensated. So do not read a non-empty ``error`` as "this run failed": read
    :attr:`status` for that. A plainly cancelled durable function leaves it ``None``."""

    tenant_id: UUID | None = None
    """Owning tenant (tagged tier); ``None`` for single-tenant deployments."""

    attempts: int = 0
    """Number of times this run has been claimed for execution (recovery increments it).

    Doubles as the **fence token**: a claim advances it under a row lock, so a later claim
    always sees a higher value. Pass it back as *fence* to a terminal write so a stale
    worker whose lease was reclaimed cannot overwrite the run (its fence no longer matches).

    It is also what a run-level retry strategy's ``max_attempts`` counts, so every claim
    spends one: a retry, a recovery claim after a crash, and a claim that could not decrypt
    the run alike. Nothing resets it.
    """

    available_at: datetime | None = None
    """Earliest instant the run may be claimed (``None`` = immediately). Set for a delayed
    run, and by :meth:`DurableRunStorePort.mark_retry` for one waiting for its retry; the
    recovery scan skips a ``PENDING`` run until it is due."""

    created_at: datetime | None = None
    """When the run was first enqueued. Populated by the store on read; ``None`` on a record
    built before persistence. Runs are ordered newest-first on ``(created_at, run_id)`` by
    :meth:`~forze.application.contracts.durable.function.DurableRunAdminPort.list_runs`."""

    cancel_requested_at: datetime | None = None
    """When an operator asked this run to stop, or ``None`` if nobody has.

    Set on the *ask*, not the landing: a ``RUNNING`` run keeps running until its lease holder
    observes the stamp, so a run can carry it while still ``RUNNING``. A ``PENDING`` run never
    claimed skips the interval — nothing has executed, so it lands ``CANCELLED`` at once. A
    ``QUARANTINED`` run, or one requeued from quarantine, carries the stamp until a claim can
    read it."""

    cancel_refused_at: datetime | None = None
    """When the run refused an observed cancel request, or ``None``.

    The one case that refuses is a durable saga past its **pivot**: forward steps must
    complete (that is what a pivot means), so the ask is recorded and declined rather than
    manufacturing a ``FORWARD_INCOMPLETE`` by operator request. A run carrying both stamps
    was asked to stop, kept going, and landed on its own merits."""

    payloads_unreadable: bool = False
    """Whether this read could not decrypt the run's payloads, its tenant's key being gone or
    its KMS failing. ``input_json`` and ``output_json`` are then ``None`` whatever the run
    holds. Only a listing returns such a run, so it can still be found, ``QUARANTINED`` ones
    included; a cancel asked of it stays recorded until recovery can decrypt it again, and the
    runner then lands it, or refuses it for a saga past its pivot."""


# ....................... #


@runtime_checkable
class DurableRunStorePort(Protocol):
    """Persist durable-run instances and hand out claims for execution/recovery.

    Single-relation, tagged-tier tenancy (a ``tenant_id`` column): recovery scans across
    tenants and re-binds each run's tenant. Per-tenant-schema (namespace) recovery is a
    future extension.

    **What a bound tenant reaches.** Every implementation answers this the same way, and the
    shared conformance battery holds them to it:

    - The verbs taking a run id — :meth:`begin`, :meth:`renew`, :meth:`load`,
      :meth:`mark_retry` and every terminal write — reach that tenant's runs **and untagged
      ones**. The untagged arm is
      what keeps a run belonging to no tenant completable: without it such a run's terminal
      write would match nothing, and it would be reclaimed and re-run forever.
    - The verbs that enumerate or control — ``claim_abandoned``, ``list_runs``,
      ``request_cancel``, ``requeue_quarantined``, ``requeue_failed``, ``purge_finished``,
      ``refuse_cancel`` — match the tenant **exactly**. Widening an enumeration is a disclosure, and none of
      them has the liveness problem above.
    - Unbound reaches everything. That is the recovery and single-tenant role, and it is
      what lets one sweep serve every tenant on a tagged relation.

    :meth:`enqueue` takes an explicit *tenant_id*: it is used for the row's tag, its scoped
    idempotency key and the relation the row lands in, all three. Passing one that
    contradicts a bound tenant is refused (``authentication`` / ``tenant_mismatch``) rather
    than applied to some of the three. Passing one with *nothing* bound is honoured — except
    on a ``tenant_aware`` store, which reads its binding first and refuses the unbound call
    (``tenant_required``): naming a tenant is not a substitute for being bound to one.
    """

    def enqueue(
        self,
        name: str,
        *,
        input_json: JsonDict | None,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
        available_at: datetime | None = None,
        coalesce_key: str | None = None,
    ) -> Awaitable[DurableRunRecord]:
        """Record a new ``PENDING`` run and return it.

        When *idempotency_key* is set and a run already exists for it, the existing run is
        returned unchanged (re-submits converge on one run). Convergence is **per tenant**:
        two tenants reusing one key stay distinct runs. *available_at* delays when the
        recovery scan may claim it (``None`` = immediately).

        When *coalesce_key* is set and a ``PENDING`` run never claimed (``attempts`` of 0)
        exists for it, that run is returned unchanged, keeping its input and its
        ``available_at``; otherwise a new run is recorded under the key. So a burst of
        submits causes one run, and a submit after the run was claimed causes another,
        which reads whatever changed meanwhile. A run waiting for a retry, or requeued, is
        never joined: its journaled steps replay rather than re-read. Atomic at the store
        (a unique key over unclaimed pending runs) and **per tenant**, like the
        idempotency key; the two cannot be combined (see :func:`check_enqueue_keys`).
        """
        ...  # pragma: no cover

    def begin(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
    ) -> Awaitable[DurableRunRecord | None]:
        """Claim a ``PENDING`` run for execution (``-> RUNNING`` + lease), or ``None``.

        Returns ``None`` when the run is not claimable (already running under a live lease,
        completed, or missing) so the caller does not double-execute it.
        """
        ...  # pragma: no cover

    def claim_abandoned(
        self,
        *,
        limit: int,
        lease_for: timedelta,
    ) -> Awaitable[Sequence[DurableRunRecord]]:
        """Claim up to *limit* abandoned runs for recovery.

        An abandoned run is a **due** ``PENDING`` run (``available_at`` in the past or unset)
        or a ``RUNNING`` run with an expired lease; each is moved to ``RUNNING`` with a fresh
        lease and an incremented attempt count (its new fence token). Concurrent scanners
        never claim the same run (``FOR UPDATE SKIP LOCKED``), so it is multi-worker-safe.

        A claimed run whose payload cannot be decrypted is left out of the result. One whose
        tenant's key is gone is held out of the scan for longer than *lease_for*, doubling with
        each claim up to a day, so such runs cannot fill every batch. On a later claim whose
        hold would reach that day it is ``QUARANTINED`` instead: its lease cleared, its
        payloads and attempts kept, and never claimed again until it is requeued. One that met
        a passing fault (a throttled KMS) keeps its lease and is never quarantined. A payload
        cipher missing from the wiring fails the call instead. A cancel asked of such a run
        stays recorded, held or quarantined with it, until a scan can decrypt it; the runner
        then lands it, or refuses it for a saga past its pivot, which must complete forward.
        Unread, it could not tell the two apart.
        """
        ...  # pragma: no cover

    def renew(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
        fence: int,
    ) -> Awaitable[DurableLeaseRenewal]:
        """Extend a running run's lease, but only while the caller still holds it.

        A long-running body calls this periodically (a heartbeat) so ``leased_until`` stays
        ahead of the recovery scanner and the run is not reclaimed while it is still
        executing. The extension applies only when the run is still ``RUNNING`` and *fence*
        (the claimed run's :attr:`DurableRunRecord.attempts`) still matches — i.e. the caller
        is the current lease holder. :attr:`DurableLeaseRenewal.held` reports the verdict:
        ``False`` means another worker reclaimed the run (a newer claim advanced
        ``attempts``), so the caller no longer owns it and must stop before its body
        double-executes the new owner's work.

        The renewal also carries :attr:`DurableLeaseRenewal.cancel_requested`, so the holder
        learns about a cancel request on the round-trip it was making anyway.
        """
        ...  # pragma: no cover

    def complete(
        self,
        run_id: str,
        *,
        output_json: JsonDict | None,
        fence: int | None = None,
    ) -> Awaitable[None]:
        """Mark a running run ``COMPLETED`` with its encoded result.

        When *fence* is given (the claimed run's :attr:`DurableRunRecord.attempts`), the
        write is a no-op unless it still matches — so a stale worker whose lease was
        reclaimed cannot complete the run out from under the new owner.
        """
        ...  # pragma: no cover

    def fail(
        self,
        run_id: str,
        *,
        error: str,
        fence: int | None = None,
    ) -> Awaitable[None]:
        """Mark a running run ``FAILED`` with a message (fenced when *fence* is given)."""
        ...  # pragma: no cover

    def mark_retry(
        self,
        run_id: str,
        *,
        error: str,
        available_at: datetime,
        fence: int | None = None,
    ) -> Awaitable[None]:
        """Send a running run back to ``PENDING`` for another attempt, due at *available_at*.

        The runner's write after a retryable failure under a retry strategy. The run keeps
        its ``run_id``, so the steps it journaled replay on its next claim, and its
        ``attempts``, which that claim advances; *error* records the failure. The lease is
        cleared, so the worker is free while the run waits. Guarded on ``RUNNING`` and fenced
        on *fence* like the terminal writes, so a stale worker cannot send back a run that
        was reclaimed.
        """
        ...  # pragma: no cover

    def mark_forward_incomplete(
        self,
        run_id: str,
        *,
        error: str,
        fence: int | None = None,
    ) -> Awaitable[None]:
        """Mark a running run ``FORWARD_INCOMPLETE`` (pivot committed, forward step failed)."""
        ...  # pragma: no cover

    def mark_cancelled(
        self,
        run_id: str,
        *,
        error: str | None = None,
        fence: int | None = None,
    ) -> Awaitable[None]:
        """Land a running run in ``CANCELLED`` — an operator asked, and it stopped.

        The **landing** half of cancellation: :meth:`DurableRunAdminPort.request_cancel`
        records the unfenced *ask*, and only the current lease holder (or the recovery claim
        that inherits the run from a dead holder) turns it into a terminal state, fenced on
        *fence* like every other terminal write. *error* carries an optional note — e.g. a
        compensation that failed while the saga rolled back — not a failure message.
        """
        ...  # pragma: no cover

    def mark_timed_out(
        self,
        run_id: str,
        *,
        error: str,
        fence: int | None = None,
    ) -> Awaitable[None]:
        """Mark a running run ``TIMED_OUT`` — it outlived its deadline (fenced when given).

        Kept distinct from :meth:`fail` because "hung past its cap" and "the body raised"
        send an operator to different places; *error* holds the deadline reason.
        """
        ...  # pragma: no cover

    def refuse_cancel(
        self,
        run_id: str,
        *,
        fence: int | None = None,
    ) -> Awaitable[None]:
        """Record that an observed cancel request was declined (see
        :attr:`DurableRunRecord.cancel_refused_at`).

        Unlike the terminal writes this is **not** guarded on ``RUNNING``: a refusal is a fact
        about the ask, not a lifecycle transition, so it is stamped whatever the run's state
        by the time it is recorded. Still fenced, so only the run's current holder can claim
        to have refused on its behalf.
        """
        ...  # pragma: no cover

    def load(
        self,
        run_id: str,
    ) -> Awaitable[DurableRunRecord | None]:
        """Return the run record, or ``None`` if unknown."""
        ...  # pragma: no cover
