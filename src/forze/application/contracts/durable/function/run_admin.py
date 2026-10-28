"""Durable-run admin / control plane: listing and run control for operator surfaces.

Kept **separate** from the operational :class:`DurableRunStorePort` (enqueue / claim /
complete) — mirroring the framework's management/data split — so a handler driving an ops
dashboard never acquires the claim/write store. Backed by the same ``durable_run`` relation.

Five verbs live here, all operator-shaped. :meth:`DurableRunAdminPort.list_runs` never
mutates a run. :meth:`DurableRunAdminPort.request_cancel` records an *ask*, deliberately not
on the data-plane store: anyone may ask a run to stop, but only the fence-holding runner may
land the terminal state. :meth:`DurableRunAdminPort.requeue_quarantined` returns runs
recovery gave up on to the scan, and :meth:`DurableRunAdminPort.requeue_failed` runs that
failed; neither has a holder, so they need no fence. :meth:`DurableRunAdminPort.purge_finished`
deletes finished runs past a retention window.
"""

from __future__ import annotations

from collections.abc import Awaitable, Collection, Sequence
from datetime import datetime, timedelta
from typing import Final, Protocol, final, runtime_checkable

import attrs

from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import CoreException, exc

from ...deps import backend_of
from .run_store import DurableRunRecord, DurableRunStatus

# ----------------------- #

_CURSOR_CODEC = B64UrlJsonCodec()
"""Opaque base64url-JSON encoder for the ``(created_at, run_id)`` keyset cursor."""

REQUEUE_UNSCOPED_CODE: Final = "durable.requeue_unscoped"
"""Code of the refusal of a requeue that names nothing it may take: an unbound, unnamed
:meth:`DurableRunAdminPort.requeue_quarantined` while a quarantined run has a tenant, or a
:meth:`DurableRunAdminPort.requeue_failed` with neither run ids nor a name."""


def requeue_unscoped_error() -> CoreException:
    """The refusal :meth:`DurableRunAdminPort.requeue_quarantined` raises for an unbound,
    unnamed call while any quarantined run belongs to a tenant."""

    return exc.validation(
        "Requeueing quarantined durable runs with no tenant bound needs run_ids while any of "
        "them belongs to a tenant: bind the tenant whose key is back, or name the runs.",
        code=REQUEUE_UNSCOPED_CODE,
    )


def requeue_failed_unnamed_error() -> CoreException:
    """The refusal :meth:`DurableRunAdminPort.requeue_failed` raises for a call naming neither
    runs nor a function."""

    return exc.validation(
        "Requeueing failed durable runs needs run_ids or a name: a run's failure history is "
        "not something to bring back whole.",
        code=REQUEUE_UNSCOPED_CODE,
    )


PURGEABLE_STATUSES: Final[tuple[DurableRunStatus, ...]] = (
    DurableRunStatus.COMPLETED,
    DurableRunStatus.CANCELLED,
    DurableRunStatus.FAILED,
    DurableRunStatus.TIMED_OUT,
)
"""The states :meth:`DurableRunAdminPort.purge_finished` may delete, and its default.

``FORWARD_INCOMPLETE`` is left out on purpose: such a run committed at its pivot and waits
for an operator to complete it by hand. So is ``QUARANTINED``, which still owes its work."""


def check_purge(
    older_than: timedelta,
    statuses: Collection[DurableRunStatus],
    limit: int,
) -> None:
    """Refuse a purge that could reach a live run or that names no bound.

    Shared by every backend, so each refuses the same calls before touching a row.

    :raises CoreException: ``validation`` for a non-positive *older_than* or *limit*, or a
        status outside :data:`PURGEABLE_STATUSES`.
    """

    if older_than <= timedelta(0):
        raise exc.validation("Durable run purge older_than must be positive.")

    if limit < 1:
        raise exc.validation("Durable run purge limit must be >= 1.")

    refused = sorted(str(status) for status in set(statuses) - set(PURGEABLE_STATUSES))

    if refused:
        raise exc.validation(
            f"Durable run purge can only delete finished runs, not {', '.join(refused)}.",
        )


# ....................... #


def encode_run_cursor(created_at: datetime, run_id: str) -> str:
    """Encode an opaque keyset cursor for newest-first ``(created_at, run_id)`` paging."""

    return _CURSOR_CODEC.dumps({"ts": created_at.isoformat(), "id": run_id})


# ....................... #


def decode_run_cursor(cursor: str) -> tuple[datetime, str]:
    """Decode a cursor from :func:`encode_run_cursor`; reject a malformed token.

    :raises CoreException: ``validation`` when *cursor* is not a token this module produced.
    """

    try:
        payload = _CURSOR_CODEC.loads(cursor)
        created_at = datetime.fromisoformat(payload["ts"])
        run_id = payload["id"]

    except (CoreException, ValueError, KeyError, TypeError) as error:
        raise exc.validation("Malformed durable-run list cursor.") from error

    # A genuine cursor is always minted from a tz-aware ``created_at`` (``utcnow``); a naive
    # timestamp can only be a hand-crafted token. Reject it rather than let a backend compare
    # it against a ``timestamptz`` in the server timezone and skip / repeat runs at the seam.
    if created_at.tzinfo is None:
        raise exc.validation("Durable-run list cursor timestamp must be timezone-aware.")

    return created_at, run_id


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableRunPage:
    """One keyset page of durable runs, newest first."""

    records: Sequence[DurableRunRecord]
    """The runs on this page (at most the requested ``limit``), newest first."""

    next_cursor: str | None = None
    """Opaque cursor to fetch the next (older) page, or ``None`` at the end of the set."""


# ....................... #


def build_run_page(records: Sequence[DurableRunRecord], limit: int) -> DurableRunPage:
    """Trim an over-fetched (``limit + 1``) newest-first list into a page + next cursor.

    A store fetches one extra record to detect a further page without a second query: when
    more than *limit* came back, the extra is dropped and the last kept record seeds the
    ``next_cursor``. Shared by every backend so paging is identical across adapters.
    """

    page = list(records[:limit])
    next_cursor = None

    if len(records) > limit and page:
        last = page[-1]

        # A further page exists, so a cursor must be minted. A boundary record without a
        # ``created_at`` (e.g. built before persistence) would otherwise truncate the listing
        # and hide older runs — fail loud instead, since a listing store always sets it.
        if last.created_at is None:
            raise exc.internal(
                "Cannot build a durable-run page cursor: the boundary record has no created_at.",
            )

        next_cursor = encode_run_cursor(last.created_at, last.run_id)

    return DurableRunPage(records=page, next_cursor=next_cursor)


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class DurableRunControlCapabilities:
    """Run-control features a :class:`DurableRunAdminPort` backend supports.

    Reported through the opt-in :class:`DurableRunControlAware` protocol. Defaults to
    ``False``: a port that cannot report its capabilities has none, so a cancel against it
    **fails closed** rather than being accepted and silently ignored. That matters more here
    than elsewhere — a "Stop" button that returns success and does nothing is worse than one
    that says it is unavailable.
    """

    supports_cancel: bool = False
    """Can :meth:`DurableRunAdminPort.request_cancel` actually reach the executing run?

    True for the self-hosted tier (mock, Postgres), where the runner observes the stamp on
    its lease heartbeat. An engine-backed tier sets it only when the engine exposes a
    cancellation mechanism the adapter maps onto."""


# ....................... #


@runtime_checkable
class DurableRunControlAware(Protocol):
    """Opt-in extension for durable-run admin ports that report their control capabilities.

    Kept off :class:`DurableRunAdminPort` so a backend opts in only when it can genuinely
    stop a run — mirroring
    :class:`~forze.application.contracts.graph.GraphStreamingAware`.
    """

    def control_capabilities(self) -> DurableRunControlCapabilities:
        """Report the run-control features this backend supports."""
        ...  # pragma: no cover


# ....................... #


def durable_run_control_capabilities(port: object) -> DurableRunControlCapabilities:
    """Report *port*'s run-control capabilities, treating a silent port as having none."""

    # The backend's report: a wrapped port forwards it, but a protocol check of the wrapper
    # never sees it.
    backend = backend_of(port)

    if isinstance(backend, DurableRunControlAware):
        return backend.control_capabilities()

    return DurableRunControlCapabilities()


# ....................... #


@runtime_checkable
class DurableRunAdminPort(Protocol):
    """Listing and run control over persisted durable runs (ops / operator surfaces).

    Newest-first keyset pagination over the same ``durable_run`` relation the store writes.
    Tenant scoping mirrors recovery: scoped to the bound tenant when one is bound, and spans
    every tenant when unbound (an operator view over a tagged shared table) — and that
    applies to :meth:`request_cancel`, :meth:`requeue_quarantined`, :meth:`requeue_failed`
    and :meth:`purge_finished` too, so an operator bound to one tenant cannot stop, requeue
    or delete a run it could not have listed.
    """

    def list_runs(
        self,
        *,
        status: DurableRunStatus | None = None,
        name: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Awaitable[DurableRunPage]:
        """Return a newest-first page of runs, filtered by *status* / *name* if given.

        Ordered by ``(created_at, run_id)`` descending (``run_id`` is a uuid7, so it breaks a
        same-instant tie in creation order). *limit* caps the page; pass the returned
        :attr:`DurableRunPage.next_cursor` back as *cursor* for the next (older) page. A
        malformed *cursor* is rejected with a ``validation`` error.

        A run whose payloads cannot be decrypted, its tenant's key being gone, is listed
        without them, with :attr:`~DurableRunRecord.payloads_unreadable` set, rather than
        failing the page; ``status=QUARANTINED`` lists the runs recovery gave up on that way.
        A passing KMS fault, or a payload cipher missing from the wiring, fails the call.
        """
        ...  # pragma: no cover

    def request_cancel(self, run_id: str) -> Awaitable[bool]:
        """Ask a run to stop; return whether the ask was recorded.

        **Cooperative, and only cooperative.** There is no in-process red button in Python: a
        thread cannot be killed, asyncio cancellation lands only at await points, and a body
        blocked inside a C extension observes nothing until it returns. This method therefore
        *requests* a stop and never guarantees when — or whether — the body notices. A body
        that needs bounded-latency stop must be structured for it (await regularly; use
        ``run_cpu`` with checkpoints at chunk boundaries). Hard kill exists only at
        process/container granularity and is a deployment concern, not a contract this port
        can honour.

        By state:

        - ``PENDING``, never claimed — transitions to ``CANCELLED`` immediately (nothing has
          executed, so there is no holder to wait for and no journal to decide by); the
          recovery scanner never claims it. Returns ``True``.
        - ``RUNNING`` — stamps :attr:`DurableRunRecord.cancel_requested_at` and returns
          ``True``. The terminal transition happens when the current lease holder observes
          the stamp on its next heartbeat, or — if the holder died — when recovery claims the
          run and lands it without invoking the body. A saga is run again instead, and
          decides by its pivot as a live one does.
        - ``QUARANTINED``, or ``PENDING`` after a requeue from it — stamps the ask, leaves the
          state alone and returns ``True``. The run may be a saga past its pivot, which must
          not be cancelled unread; the ask is decided once a claim can decrypt it.
        - ``PENDING`` waiting for a retry, or requeued after it failed (``attempts`` of one or
          more) — stamps the ask and returns ``True``. It lands when the run comes due, by
          the claim that would have run it, which spends an attempt.
        - terminal — no-op, returns ``False``.
        - unknown, or owned by another tenant while one is bound — no-op, returns ``False``
          (indistinguishable on purpose: an operator scoped to one tenant learns nothing
          about the existence of another's runs).

        The ask is **unfenced**: anyone may ask, and asking twice changes nothing. Only the
        fence-holding runner may land ``CANCELLED``, so a stale worker cannot cancel a run
        out from under its new owner.

        Backends report through :class:`DurableRunControlAware` whether they can honour this
        at all; check :func:`durable_run_control_capabilities` before offering it — as
        :meth:`~forze_kits.integrations.durable.DurableFunctionRunner.request_cancel` does,
        so an operator surface built on the runner is gated for free.

        **Adapter obligation: a backend that cannot deliver a cancel must raise
        ``configuration``, never return ``False``.** ``False`` is already spoken for — it
        means *there was nothing to stop* (terminal, unknown, or another tenant's run). An
        adapter that reuses it for *nothing here will ever stop* makes those two
        indistinguishable to the caller, and a caller who cannot tell them apart shows a
        "Stop" button that reports success and does nothing. That is the failure this whole
        surface exists to avoid, arriving through the return type instead of the behaviour.
        """
        ...  # pragma: no cover

    def requeue_quarantined(
        self,
        *,
        run_ids: Sequence[str] | None = None,
    ) -> Awaitable[int]:
        """Return ``QUARANTINED`` runs to ``PENDING``; return how many moved.

        Call it once the keys those runs need are back, and name what to bring back: bind
        the tenant whose key is back, or pass *run_ids*. A requeued run keeps its
        ``created_at``, so the scan, oldest first, claims it ahead of every fresh run, and
        one whose key is still gone costs a claim and a KMS call before it is quarantined
        again at once, its ``attempts`` already putting its hold at the cap. Each run is due
        at once, with no lease, and keeps its ``attempts``, payloads and cancel stamps.

        Scoped like :meth:`list_runs`: bound, only that tenant's runs; unbound with
        *run_ids*, the named runs of any tenant.

        :param run_ids: Requeue only these runs, skipping any not quarantined or out of
            scope. ``None`` names every quarantined run in scope; an empty sequence, none.
        :returns: How many runs moved.
        :raises CoreException: ``validation`` (:data:`REQUEUE_UNSCOPED_CODE`) for an unbound
            call without *run_ids* while any quarantined run belongs to a tenant. A
            deployment whose runs carry no tenant requeues unbound.
        """
        ...  # pragma: no cover

    def requeue_failed(
        self,
        *,
        run_ids: Sequence[str] | None = None,
        name: str | None = None,
    ) -> Awaitable[int]:
        """Return ``FAILED`` and ``TIMED_OUT`` runs to ``PENDING``; return how many moved.

        Call it once whatever failed them is fixed, naming the runs or the function. A
        requeued run keeps its ``run_id``, so the steps it journaled replay on its next claim
        and only the rest run again. It is due at once, with no lease, and keeps its payloads,
        its ``error`` until it lands again, and its ``attempts``: they are its fence, and count
        toward a retry strategy's ``max_attempts``, so a run that spent them gets one attempt
        per requeue. It keeps ``created_at`` too, so the scan, oldest first, claims requeued
        runs ahead of fresh ones.

        A saga journals a failed forward step, so a requeue of one that rolled back
        (``saga.step_failed``) fails the same way again: such work needs a new run. A requeue
        of one whose rollback failed runs again only the compensations that failed.

        Left where they are: ``CANCELLED`` runs, since an operator stopped them;
        ``FORWARD_INCOMPLETE`` ones, which committed at their pivot and are completed by hand;
        and a failed run whose cancel ask is still unanswered, which honours the ask.

        Scoped like :meth:`list_runs`: bound, only that tenant's runs; unbound, any tenant's.
        Unlike :meth:`requeue_quarantined` an unbound call may name a function across tenants:
        a failed run opens like any other, so bringing it back puts no unreadable run at the
        head of the scan.

        :param run_ids: Requeue only these runs, skipping any not failed, timed out or in
            scope; an empty sequence names none.
        :param name: Requeue only runs of this function.
        :returns: How many runs moved.
        :raises CoreException: ``validation`` (:data:`REQUEUE_UNSCOPED_CODE`) for a call with
            neither *run_ids* nor *name*.
        """
        ...  # pragma: no cover

    def purge_finished(
        self,
        older_than: timedelta,
        *,
        statuses: Collection[DurableRunStatus] = PURGEABLE_STATUSES,
        name: str | None = None,
        limit: int = 1000,
    ) -> Awaitable[int]:
        """Delete up to *limit* finished runs created more than *older_than* ago; return how
        many were deleted.

        Oldest first. Each deleted run takes its step journal with it when the store knows
        the journal's relation, as one wired through a deps module does. Only the states in
        :data:`PURGEABLE_STATUSES` can be named, so a run that still owes work is never
        deleted; a ``FAILED`` run requeued while the purge runs is kept, with its journal.

        Purging a run frees its idempotency key: a later submit under it starts a new run.

        Scoped like :meth:`list_runs`: bound, only that tenant's runs; unbound, any tenant's.

        :param older_than: Keep runs created within this window.
        :param statuses: Delete only runs in these states.
        :param name: Delete only runs of this function.
        :param limit: The most runs one call deletes.
        :returns: How many runs were deleted.
        :raises CoreException: ``validation`` from :func:`check_purge`.
        """
        ...  # pragma: no cover
