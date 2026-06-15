"""Shared field-encryption resolution for search adapters.

Every search backend — external index (Meilisearch) or in-place over an encrypted
document table (Postgres FTS/vector, Mongo text/vector) — uses this to wrap a
:class:`SearchSpec`'s read codec with an :class:`EncryptingModelCodec`, so encrypted
fields are decrypted out of search results (and, for an external index, sealed on upsert).

The wrapped codec uses the **default** field-encryption AAD label and the spec's record-id
binding, so an in-place search reproduces exactly the document write's configuration and
decrypts the document's own ciphertext. The spec's ``encrypted_fields`` / ``searchable_fields``
/ ``encryption_binds_record_id`` must therefore match the underlying ``DocumentSpec``.
"""

from collections.abc import Callable

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    FieldCipherPort,
)
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import EncryptingModelCodec
from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD

# ----------------------- #

_WIRING_CODE = "core.search.encryption_wiring"


def resolve_search_read_codec_spec[M: BaseModel](
    spec: SearchSpec[M],
    *,
    keyring: FieldCipherPort | None,
    deterministic: DeterministicFieldCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
) -> SearchSpec[M]:
    """Return *spec* with its read codec wrapped for field encryption, or unchanged.

    Fail-closed: declaring encrypted/searchable fields without the matching cipher wired
    raises rather than silently returning ciphertext.
    """

    if not (spec.encrypted_fields or spec.searchable_fields):
        return spec

    if keyring is None:
        raise exc.configuration(
            f"SearchSpec {spec.name!r} declares encrypted/searchable fields but no keyring "
            "is wired. Register a CryptoDepsModule or clear the encrypted fields.",
            code=_WIRING_CODE,
        )

    if spec.searchable_fields and deterministic is None:
        raise exc.configuration(
            f"SearchSpec {spec.name!r} declares searchable_fields but no deterministic "
            "cipher is wired (CryptoDepsModule(deterministic_root=...)).",
            code=_WIRING_CODE,
        )

    wrapped = EncryptingModelCodec(
        inner=spec.resolved_read_codec,
        cipher=keyring,
        fields=spec.encrypted_fields,
        searchable_fields=spec.searchable_fields,
        deterministic=deterministic,
        tenant_provider=tenant_provider,
        record_id_field=ID_FIELD if spec.encryption_binds_record_id else None,
    )

    return attrs.evolve(spec, read_codec=wrapped)
