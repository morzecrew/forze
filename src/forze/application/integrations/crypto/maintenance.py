"""Maintenance helpers for encrypted data — re-encryption / rotation sweeps."""

from collections.abc import Callable
from typing import Any

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.storage import StorageCommandPort, StorageQueryPort

# ----------------------- #


async def reencrypt_documents(
    query: DocumentQueryPort[Any],
    command: DocumentCommandPort[Any, Any, Any, Any],
    *,
    to_update: Callable[[Any], Any],
    filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
    chunk_size: int = 500,
) -> int:
    """Re-write matching documents so their encrypted fields are re-encrypted.

    Streams the documents (decrypted on read) and writes each back via an update
    built by *to_update* — the read→write round-trip re-encrypts under the current
    keys. The migration / break-glass primitive behind key rotation:

    - **Randomized fields** get fresh envelopes (new data key / nonce) — use this
      after a suspected key compromise. (Routine KEK rotation needs no sweep —
      envelopes are self-describing.)
    - **Searchable (deterministic) fields** only change when their key changed, so
      this is the re-index step of a searchable-key rotation (run it with the new
      key active and the old key still readable).

    *to_update* maps a read model to an update DTO carrying the field values to
    re-encrypt, e.g. ``lambda d: CustomerUpdate(email=d.email)``. Best-effort: a
    row modified concurrently mid-sweep raises a revision conflict — rerun, or wrap
    the call with retry. Returns the number of documents re-written.
    """

    count = 0

    async for batch in query.find_stream(filters, chunk_size=chunk_size):
        for doc in batch:
            await command.update(doc.id, doc.rev, to_update(doc))
            count += 1

    return count


# ....................... #


async def reencrypt_objects(
    query: StorageQueryPort,
    command: StorageCommandPort,
    *,
    prefix: str | None = None,
    page_size: int = 100,
) -> int:
    """Re-write every stored object so its payload is re-encrypted, in place.

    The object-storage counterpart of :func:`reencrypt_documents`: each object is
    streamed down (decrypted on read) and streamed back to **the same key** (re-sealed
    on write under a fresh data key), so peak memory is one chunk however large the
    object is. The break-glass primitive behind key rotation for blobs — routine KEK
    rotation needs no sweep, since envelopes are self-describing; run this after a
    suspected key compromise, or as the migration step when a route's key changes.

    Re-writing the *same* key is what keeps this possible at all: an object's encryption
    AAD binds it to ``(bucket, key)``, so a copy to a different key could not be
    decrypted. ``content_type``, user metadata, and tags are carried over from each
    object's ``head`` so the round-trip preserves them.

    On a route with no cipher this is a faithful (wasteful) rewrite — point it at an
    encrypting route. Best-effort and resumable: objects are re-written one at a time and
    each is atomic, so an interrupted sweep can simply be re-run (an already-re-encrypted
    object is just re-encrypted again). Returns the number of objects re-written.

    The route's keys are enumerated up front — one string each, not the payloads — so a
    rewrite can never perturb the pagination that is still walking over it.
    """

    # Enumerate first, rewrite second. Paging and mutation must not interleave: the
    # storage contract makes no promise about *how* ``list`` orders its results, so a
    # backend that orders by anything a rewrite touches (last-modified, say) would
    # reshuffle each object as the sweep passed over it and an advancing offset would
    # then skip keys — silently leaving blobs behind under the old key. Listing is
    # read-only, so paging it to exhaustion is stable; only the keys are held (the
    # payloads are still streamed one chunk at a time below).
    keys: list[str] = []
    offset = 0

    while True:
        page, _ = await query.list(page_size, offset, prefix=prefix)

        if not page:
            break

        keys.extend(item.key for item in page)
        offset += len(page)

    count = 0

    for key in keys:
        head = await query.head(key, include_tags=True)
        streamed = await query.download_stream(key)

        await command.overwrite_stream(
            key,
            streamed.chunks,
            content_type=head.content_type,
            metadata=head.metadata,
            tags=head.tags,
        )
        count += 1

    return count
