"""Backfill an external search index from the document plane it indexes.

The two index-sync bindings (``bind_search_sync``, ``bind_search_sync_outbox``) are both
**incremental**: they carry a row into the index when that row is *written*. Nothing carries
a row that was never written since the index existed — so a freshly provisioned index, an
aggregate that gained ``search=…`` after it already held rows, an index restored onto new
infrastructure, and an index that drifted while its sync was broken all end up the same way:
correct-looking, and empty (or stale) for every untouched row.

This is the sweep that fills them. It streams the document plane and applies each row to the
index under **the same rule the incremental syncs apply** — live rows upserted, soft-deleted
rows removed — so a rebuilt index converges to exactly what an unbroken sync would have
produced. That equivalence is the whole contract: keep it in step with :mod:`.sync` and
:mod:`.outbox_sync`, or a rebuild silently produces a *different* index than the one the app
has been maintaining.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.integrations.search import assert_search_encryption_parity
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentQueryPort, DocumentSpec
    from forze.application.contracts.querying import QueryFilterExpression
    from forze.application.contracts.search import SearchCommandPort, SearchSpec

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SearchRebuildReport:
    """Outcome of a search-index rebuild sweep.

    Every row the sweep read was applied to the index one way or the other, so the two counts
    partition the scan: a row is either in the index now (:attr:`indexed`) or provably not
    (:attr:`removed`). Keeping them apart is what lets an operator read a result: a rebuild
    reporting ``indexed=0, removed=4000`` over an aggregate that should be live is a
    soft-delete flag gone wrong, not an empty collection — and a single ``scanned`` total
    would have hidden that.
    """

    indexed: int
    """Live rows upserted into the index."""

    removed: int
    """Soft-deleted rows deleted from the index (they must not be searchable)."""

    @property
    def scanned(self) -> int:
        """Rows read from the document plane — each was either indexed or removed."""

        return self.indexed + self.removed


# ....................... #


async def rebuild_search_index(
    query: DocumentQueryPort[Any],
    command: SearchCommandPort[Any],
    *,
    document: DocumentSpec[Any, Any, Any, Any],
    search: SearchSpec[Any],
    filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
    chunk_size: int = 500,
) -> SearchRebuildReport:
    """Stream *document*'s rows into *search*, applying the index-sync rule to each.

    A keyset-paged loop over ``find_stream`` — bounded memory whatever the collection's size
    — upserting each chunk's live rows in one ``upsert_many`` and deleting each chunk's
    soft-deleted ids in one ``delete``. The delete half is what makes the sweep a *rebuild*
    rather than a fill: a soft-deleted row still exists in the document plane, and the
    incremental sync removes it from the index rather than upserting it, so a sweep that
    merely upserted everything it read would resurrect every soft-deleted row as a searchable
    ghost — a hit that ``GET`` then 404s. Rows whose read model has no soft-delete flag are
    upserted unconditionally (the same inertness as the sync path's ``getattr`` guard).

    **Source-driven, so it converges the index toward the document plane rather than
    replacing it.** Every row the document plane still holds ends up correct. An id the
    document plane no longer holds *at all* — hard-deleted while the index kept it — is
    invisible to a sweep that reads only the source, and survives. Where that matters (an
    index of unknown provenance, rather than a fresh one), wipe first via
    ``SearchManagementPort.delete_all()`` and rebuild into the empty index; the sweep is
    deliberately not given that port, because the wipe leaves search returning nothing until
    the sweep finishes and that is the caller's outage to choose, not this function's to take.

    Idempotent and resumable: applying a row twice is applying it once, so an interrupted
    sweep is re-run, not repaired. Best-effort against live traffic — a row hard-deleted
    between this sweep reading it and upserting it is re-added as a ghost, so an *exact*
    result wants a source that is not being written (a fresh import, or a quiesced runtime).
    Concurrent traffic on a *synced* aggregate converges anyway, since the sync is applying
    the same rule to those same writes.

    Tenancy rides on the ambient identity exactly as the ports do: both ports are already
    bound to a tenant when the caller resolved them, so a per-tenant rebuild is this call
    under ``bind_identity(tenant=…)``, once per tenant.
    """

    # The sweep feeds the index the document's *decrypted* read model, so it is one more seam
    # that holds both specs — and therefore one more place the encryption drift that would
    # write sealed fields to an external index in clear has to be refused. Checked before the
    # first row is read, so a mismatch costs nothing and can never half-fill an index.
    assert_search_encryption_parity(document=document, search=search)

    indexed = 0
    removed = 0

    async for batch in query.find_stream(filters, chunk_size=chunk_size):
        live: list[Any] = []
        deleted_ids: list[str] = []

        for row in batch:
            if getattr(row, SOFT_DELETE_FIELD, False):
                deleted_ids.append(str(row.id))

            else:
                live.append(row)

        if live:
            await command.upsert_many(live)
            indexed += len(live)

        if deleted_ids:
            await command.delete(deleted_ids)
            removed += len(deleted_ids)

    return SearchRebuildReport(indexed=indexed, removed=removed)
