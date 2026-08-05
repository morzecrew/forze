"""Durable-run admin / control plane: listing and run control for operator surfaces.

Kept **separate** from the operational :class:`DurableRunStorePort` (enqueue / claim /
complete) — mirroring the framework's management/data split — so a handler driving an ops
dashboard never acquires the claim/write store. Backed by the same ``durable_run`` relation.

Two verbs live here, both operator-shaped. :meth:`DurableRunAdminPort.list_runs` never
mutates a run. :meth:`DurableRunAdminPort.request_cancel` records an *ask* — deliberately
the only operator verb that writes, and deliberately not on the data-plane store: anyone may
ask a run to stop, but only the fence-holding runner may land the terminal state.
"""

from __future__ import annotations

from collections.abc import Awaitable, Sequence
from datetime import datetime
from typing import Protocol, final, runtime_checkable

import attrs

from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import CoreException, exc

from .run_store import DurableRunRecord, DurableRunStatus

# ----------------------- #

_CURSOR_CODEC = B64UrlJsonCodec()
"""Opaque base64url-JSON encoder for the ``(created_at, run_id)`` keyset cursor."""


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

    if isinstance(port, DurableRunControlAware):
        return port.control_capabilities()

    return DurableRunControlCapabilities()


# ....................... #


@runtime_checkable
class DurableRunAdminPort(Protocol):
    """Listing and run control over persisted durable runs (ops / operator surfaces).

    Newest-first keyset pagination over the same ``durable_run`` relation the store writes.
    Tenant scoping mirrors recovery: scoped to the bound tenant when one is bound, and spans
    every tenant when unbound (an operator view over a tagged shared table) — and that
    applies to :meth:`request_cancel` too, so an operator bound to one tenant cannot stop a
    run it could not have listed.
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

        - ``PENDING`` — transitions to ``CANCELLED`` immediately (nothing is executing, so
          there is no holder to wait for); the recovery scanner never claims it. Returns
          ``True``.
        - ``RUNNING`` — stamps :attr:`DurableRunRecord.cancel_requested_at` and returns
          ``True``. The terminal transition happens when the current lease holder observes
          the stamp on its next heartbeat, or — if the holder died — when recovery claims the
          run and lands it without invoking the body.
        - terminal — no-op, returns ``False``.

        The ask is **unfenced**: anyone may ask, and asking twice changes nothing. Only the
        fence-holding runner may land ``CANCELLED``, so a stale worker cannot cancel a run
        out from under its new owner.

        Backends report through :class:`DurableRunControlAware` whether they can honour this
        at all; check :func:`durable_run_control_capabilities` before offering it, or a
        request against a tier without a cancellation mechanism is accepted and dropped.
        """
        ...  # pragma: no cover
