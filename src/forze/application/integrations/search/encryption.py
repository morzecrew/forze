"""Shared field-encryption resolution for search adapters.

Every search backend — external index (Meilisearch) or in-place over an encrypted
document table (Postgres FTS/vector, Mongo text/vector) — uses this to wrap a
:class:`SearchSpec`'s read codec with an :class:`EncryptingModelCodec`, so encrypted
fields are decrypted out of search results (and, for an external index, sealed on upsert).

The wrapped codec uses the **default** field-encryption AAD label and the spec's record-id
binding, so an in-place search reproduces exactly the document write's configuration and
decrypts the document's own ciphertext. The spec's ``encryption`` policy must therefore be
the same as the underlying ``DocumentSpec.encryption``.
"""

from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import Any, Protocol, cast

import attrs

from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    FieldCipherPort,
    FieldEncryption,
    KeyringPort,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import EncryptingModelCodec, decrypt_rows
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD

# ----------------------- #

_WIRING_CODE = "core.search.encryption_wiring"


def search_spec_encrypts(spec: object) -> bool:
    """Whether a search/hub spec declares non-empty field encryption.

    Drives snapshot sealing: a route that decrypts confidential fields into its result models
    must seal those models in the snapshot store too (else the at-rest re-exposure the document
    sealing prevents reappears in the snapshot). Federated routes seal when **any** member does.
    """

    encryption = getattr(spec, "encryption", None)
    return encryption is not None and not encryption.is_empty


def reject_encrypted_sort_fields(
    sorts: Mapping[str, Any] | None,
    *,
    encryption: FieldEncryption | None,
    spec_name: str | StrEnum,
) -> None:
    """Refuse a sort key that names an encrypted or searchable field.

    A field-encrypted column has no usable order at rest: a ``encrypted`` (randomized)
    ciphertext is unordered, and a ``searchable`` (deterministic) ciphertext supports
    equality only — never range/sort. Sorting on either is therefore meaningless, and worse,
    keyset cursors carry the last row's *raw* sort value (the ciphertext, base64'd JSON, not
    sealed) in the cursor token — exposing confidential field values to anyone inspecting the
    token. Reject such sorts fail-closed at the query seam so the leak and the latent
    no-op-sort bug both close.
    """

    if not sorts or encryption is None:
        return

    if forbidden := encryption.forbidden_sort_fields(sorts):
        raise exc.precondition(
            f"Sorting on field-encrypted field(s) {forbidden} is not allowed for "
            f"{spec_name!r}: encrypted (randomized) and searchable (deterministic) "
            "fields have no order at rest and cannot be used as sort keys.",
            code="core.search.encrypted_sort_field",
        )


def resolve_snapshot_cipher(*, encrypted: bool, keyring: KeyringPort | None) -> KeyringPort | None:
    """Cipher for sealing snapshot records, fail-closed when one is required but unwired.

    A route that field-encrypts must not silently snapshot plaintext, so an encrypted route
    without a wired keyring raises rather than degrading to ``cipher=None``. Returns ``None``
    (plaintext snapshot) only when the route does not encrypt.
    """

    if not encrypted:
        return None

    if keyring is None:
        raise exc.configuration(
            "A search route's result snapshot requires field encryption but no keyring is "
            "wired. Register a CryptoDepsModule or clear the route's encryption.",
            code="core.search.snapshot_encryption_wiring",
        )

    return keyring


class _EncryptableReadSpec(Protocol):
    """A search spec that can declare field encryption (``SearchSpec``/``HubSearchSpec``)."""

    @property
    def name(self) -> str | StrEnum: ...
    @property
    def encryption(self) -> FieldEncryption | None: ...
    @property
    def resolved_read_codec(self) -> ModelCodec[Any, Any]: ...


def resolve_search_read_codec_spec[S: _EncryptableReadSpec](
    spec: S,
    *,
    keyring: FieldCipherPort | None,
    deterministic: DeterministicFieldCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
) -> S:
    """Return *spec* with its read codec wrapped for field encryption, or unchanged.

    Fail-closed: declaring encrypted/searchable fields without the matching cipher wired
    raises rather than silently returning ciphertext.
    """

    encryption = spec.encryption
    if encryption is None or encryption.is_empty:
        return spec

    if keyring is None:
        raise exc.configuration(
            f"SearchSpec {spec.name!r} declares encrypted/searchable fields but no keyring "
            "is wired. Register a CryptoDepsModule or clear the encrypted fields.",
            code=_WIRING_CODE,
        )

    if encryption.searchable and deterministic is None:
        raise exc.configuration(
            f"SearchSpec {spec.name!r} declares searchable fields but no deterministic "
            "cipher is wired (CryptoDepsModule(deterministic_root=...)).",
            code=_WIRING_CODE,
        )

    wrapped = EncryptingModelCodec(
        inner=spec.resolved_read_codec,
        cipher=keyring,
        fields=encryption.encrypted,
        searchable_fields=encryption.searchable,
        deterministic=deterministic,
        tenant_provider=tenant_provider,
        record_id_field=ID_FIELD if encryption.binds_record_id else None,
        reject_plaintext=encryption.reject_plaintext,
    )

    # ``spec`` is always a concrete attrs spec (SearchSpec / HubSearchSpec) at runtime;
    # the Protocol bound just can't express "is an attrs class" to evolve's signature.
    return cast(S, attrs.evolve(cast(Any, spec), read_codec=wrapped))


async def decrypt_search_rows(
    codec: ModelCodec[Any, Any], rows: list[JsonDict]
) -> tuple[list[JsonDict], ModelCodec[Any, Any]]:
    """Decrypt sealed fields in raw search rows **once**, before any decode.

    Every search read path (offset, cursor, ...) calls this right after fetching rows, so
    the spec model, a custom ``return_type``, and raw field projections all receive
    plaintext. A thin search-named alias over the shared :func:`decrypt_rows` primitive.
    """

    return await decrypt_rows(codec, rows)
