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

from forze.base.primitives import JsonDict

# ----------------------- #


class DurableRunStatus(StrEnum):
    """Lifecycle state of a durable run."""

    PENDING = "pending"
    """Enqueued, not yet claimed for execution."""

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
    ``FAILED``/``FORWARD_INCOMPLETE``/``TIMED_OUT``.

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
    """

    available_at: datetime | None = None
    """Earliest instant the run may be claimed (``None`` = immediately). Set for a delayed
    run; the recovery scan skips a ``PENDING`` run until it is due."""

    created_at: datetime | None = None
    """When the run was first enqueued. Populated by the store on read; ``None`` on a record
    built before persistence. Runs are ordered newest-first on ``(created_at, run_id)`` by
    :meth:`~forze.application.contracts.durable.function.DurableRunAdminPort.list_runs`."""

    cancel_requested_at: datetime | None = None
    """When an operator asked this run to stop, or ``None`` if nobody has.

    Set on the *ask*, not the landing: a ``RUNNING`` run keeps running until its lease holder
    observes the stamp, so a run can carry it while still ``RUNNING``. ``PENDING`` runs skip
    the interval — nothing is executing, so they land ``CANCELLED`` at once."""

    cancel_refused_at: datetime | None = None
    """When the run refused an observed cancel request, or ``None``.

    The one case that refuses is a durable saga past its **pivot**: forward steps must
    complete (that is what a pivot means), so the ask is recorded and declined rather than
    manufacturing a ``FORWARD_INCOMPLETE`` by operator request. A run carrying both stamps
    was asked to stop, kept going, and landed on its own merits."""


# ....................... #


@runtime_checkable
class DurableRunStorePort(Protocol):
    """Persist durable-run instances and hand out claims for execution/recovery.

    Single-relation, tagged-tier tenancy (a ``tenant_id`` column): recovery scans across
    tenants and re-binds each run's tenant. Per-tenant-schema (namespace) recovery is a
    future extension.
    """

    def enqueue(
        self,
        name: str,
        *,
        input_json: JsonDict | None,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
        available_at: datetime | None = None,
    ) -> Awaitable[DurableRunRecord]:
        """Record a new ``PENDING`` run and return it.

        When *idempotency_key* is set and a run already exists for it, the existing run is
        returned unchanged (re-submits converge on one run). Convergence is **per tenant**:
        two tenants reusing one key stay distinct runs. *available_at* delays when the
        recovery scan may claim it (``None`` = immediately).
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
