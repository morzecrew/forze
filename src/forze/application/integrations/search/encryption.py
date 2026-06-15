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

from collections.abc import Callable
from enum import StrEnum
from typing import Any, Protocol, cast

import attrs

from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    FieldCipherPort,
    FieldEncryption,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import EncryptingModelCodec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD

# ----------------------- #

_WIRING_CODE = "core.search.encryption_wiring"


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
    plaintext — decryption belongs to the row, not to one decode path. Returns the rows
    and the codec to decode them with: the plain inner codec on decryption (the encrypting
    codec would re-attempt it), or the codec unchanged when it is not an encrypting one.
    """

    decrypt_mapping = getattr(codec, "decrypt_mapping", None)

    if decrypt_mapping is None:
        return rows, codec

    prepare_decrypt = getattr(codec, "prepare_decrypt", None)
    if prepare_decrypt is not None:
        await prepare_decrypt(rows)

    decrypted = [decrypt_mapping(dict(row)) for row in rows]
    return decrypted, getattr(codec, "inner", codec)
