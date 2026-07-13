"""Field-encryption parity between a document aggregate and the search index over it."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from forze.application.contracts.crypto import FieldEncryption
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.contracts.search import SearchSpec

# ----------------------- #


def _sealed(encryption: FieldEncryption | None) -> frozenset[str]:
    if encryption is None:
        return frozenset()

    return encryption.encrypted | encryption.searchable


# ....................... #


def assert_search_encryption_parity(
    *,
    document: DocumentSpec[Any, Any, Any, Any],
    search: SearchSpec[Any],
) -> None:
    """Fail closed unless *search* declares the **same** field-encryption policy as *document*.

    A search index over a document aggregate is fed the aggregate's own read model — the
    decrypted, in-memory one. The index's codec is what re-seals it, and it seals exactly
    what ``SearchSpec.encryption`` names. So a field the document seals but the search spec
    omits is written to the external index (Meilisearch) **in clear**: the document's own
    sealing is intact and the leak is entirely in the drift between the two declarations. For
    in-place search (Postgres / Mongo over the encrypted table) the same drift breaks
    decryption instead — the AAD no longer reproduces the document write's.

    The two specs are two projections of one policy, so parity is the rule; this is the seam
    that co-locates them, which is the only place the rule can be checked at all.
    """

    if document.encryption == search.encryption:
        return

    doc_sealed = _sealed(document.encryption)
    search_sealed = _sealed(search.encryption)
    leaked = doc_sealed - search_sealed

    detail = (
        f"fields sealed on the document but not on the search spec: {sorted(leaked)} — "
        f"they would be written to the index in clear"
        if leaked
        else (
            f"the field sets agree ({sorted(doc_sealed)}) but the policies differ "
            f"(binds_record_id / reject_plaintext / encrypted-vs-searchable split)"
        )
    )

    raise exc.configuration(
        f"Search spec {search.name!r} and document spec {document.name!r} declare different "
        f"field encryption, but the index is fed the document's decrypted read model: "
        f"{detail}. Point both specs at the same FieldEncryption policy object.",
        code="search_encryption_parity_mismatch",
    )
