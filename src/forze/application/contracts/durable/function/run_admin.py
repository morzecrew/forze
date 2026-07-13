"""Durable-run admin / query plane: read-only listing for operator surfaces.

Kept **separate** from the operational :class:`DurableRunStorePort` (enqueue / claim /
complete) — mirroring the framework's management/data split — so a read-only handler (e.g. a
CQRS ``QUERY``) can list runs for an ops dashboard without acquiring the claim/write store.
Backed by the same ``durable_run`` relation; listing never mutates a run.
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


@runtime_checkable
class DurableRunAdminPort(Protocol):
    """Read-only listing over persisted durable runs (ops / operator surfaces).

    Newest-first keyset pagination over the same ``durable_run`` relation the store writes.
    Tenant scoping mirrors recovery: scoped to the bound tenant when one is bound, and spans
    every tenant when unbound (an operator view over a tagged shared table).
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
