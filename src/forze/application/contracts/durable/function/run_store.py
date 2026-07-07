"""Durable-run store contract: the run/instance record backing crash recovery.

A durable run is one invocation of a registered durable function (or saga). The store
persists the run instance so a crashed run can be re-claimed and resumed — the step-memo
journal (:class:`DurableFunctionStepPort`) then replays completed steps and the first
incomplete step runs live. Backend-agnostic: implemented over Postgres (self-hosted) and
an in-memory mock (tests / simulation).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Awaitable, Protocol, Sequence, final, runtime_checkable
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
    """Failure message, present once :attr:`status` is ``FAILED``/``FORWARD_INCOMPLETE``."""

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
        idempotency_key: str | None = None,  # noqa: F841
        tenant_id: UUID | None = None,  # noqa: F841
        available_at: datetime | None = None,  # noqa: F841
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
        fence: int,  # noqa: F841
    ) -> Awaitable[bool]:
        """Extend a running run's lease, but only while the caller still holds it.

        A long-running body calls this periodically (a heartbeat) so ``leased_until`` stays
        ahead of the recovery scanner and the run is not reclaimed while it is still
        executing. The extension applies only when the run is still ``RUNNING`` and *fence*
        (the claimed run's :attr:`DurableRunRecord.attempts`) still matches — i.e. the caller
        is the current lease holder. Returns whether the lease was extended: ``False`` means
        another worker reclaimed the run (a newer claim advanced ``attempts``), so the caller
        no longer owns it and must stop before its body double-executes the new owner's work.
        """
        ...  # pragma: no cover

    def complete(
        self,
        run_id: str,
        *,
        output_json: JsonDict | None,
        fence: int | None = None,  # noqa: F841
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
        fence: int | None = None,  # noqa: F841
    ) -> Awaitable[None]:
        """Mark a running run ``FAILED`` with a message (fenced when *fence* is given)."""
        ...  # pragma: no cover

    def mark_forward_incomplete(
        self,
        run_id: str,
        *,
        error: str,
        fence: int | None = None,  # noqa: F841
    ) -> Awaitable[None]:
        """Mark a running run ``FORWARD_INCOMPLETE`` (pivot committed, forward step failed)."""
        ...  # pragma: no cover

    def load(
        self,
        run_id: str,
    ) -> Awaitable[DurableRunRecord | None]:
        """Return the run record, or ``None`` if unknown."""
        ...  # pragma: no cover
