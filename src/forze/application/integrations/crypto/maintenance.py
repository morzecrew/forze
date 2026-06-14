"""Maintenance helpers for encrypted documents — re-encryption / rotation sweeps."""

from collections.abc import Callable
from typing import Any

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
)
from forze.application.contracts.querying import QueryFilterExpression

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
