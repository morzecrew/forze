"""Field-encryption parity between a document aggregate and the search index over it."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from forze.application.contracts.crypto import FieldEncryption
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.contracts.search import SearchSpec

# ----------------------- #


_NO_ENCRYPTION = FieldEncryption()
"""The canonical "nothing is sealed" policy.

``None`` and an empty :class:`FieldEncryption` are the same declaration — both seal zero
fields — so they must compare equal. Normalizing here keeps the parity check on *policy*
rather than on how the author happened to spell its absence.
"""


def _policy(encryption: FieldEncryption | None) -> FieldEncryption:
    return _NO_ENCRYPTION if encryption is None else encryption


def _sealed(encryption: FieldEncryption) -> frozenset[str]:
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
    that co-locates them, which is the only place the rule can be checked at all. Declaring no
    encryption at all is parity too, however it is spelled: ``None`` and an empty
    ``FieldEncryption()`` seal the same zero fields, so a plain unencrypted aggregate passes.
    """

    document_policy = _policy(document.encryption)
    search_policy = _policy(search.encryption)

    if document_policy == search_policy:
        return

    doc_sealed = _sealed(document_policy)
    search_sealed = _sealed(search_policy)
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
