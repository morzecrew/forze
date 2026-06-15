"""Shared field-encryption resolution for graph adapters.

Graph nodes and edges carry confidential properties that should be sealed at rest. Unlike a
document (one keyed read/write model), a graph module has many node/edge *kinds*, each with its
own read model, create/update DTOs, and ``key_field`` — and reads/writes flow through raw
property maps, not one model. So encryption is resolved per kind and driven at the **mapping**
level: :meth:`EncryptingModelCodec.encrypt_mapping` seals a property dict on write,
:meth:`EncryptingModelCodec.decrypt_mapping` opens it on read.

Encrypted properties are confidential — decrypted out of every read path (get / neighbors /
walk / shortest-path / scoped-walk) but **not** matchable in traversal predicates: randomized
ciphertext has no structure, and structural traversal is unaffected. ``binds_record_id`` binds
the kind's ``key_field`` (rejected for ``identity="endpoints"`` edges, which have no per-edge id).
"""

from collections.abc import Callable
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    DeterministicFieldCipherPort,
    FieldCipherPort,
    FieldEncryption,
)
from forze.application.contracts.graph import GraphModuleSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import EncryptingModelCodec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec, default_model_codec

# ----------------------- #

_WIRING_CODE = "core.graph.encryption_wiring"


@attrs.define(slots=True, frozen=True, kw_only=True)
class GraphKindCipher:
    """Per-kind property-map encryption for one graph node or edge kind.

    Wraps the read codec (for decode) and an optional :class:`EncryptingModelCodec` (for the
    field crypto). When ``cipher`` is ``None`` the kind is plaintext and both methods are the
    plain encode/decode.
    """

    read_codec: ModelCodec[Any, Any]
    cipher: EncryptingModelCodec[Any] | None

    # ....................... #

    async def seal(self, props: JsonDict, *, record_id: Any = None) -> JsonDict:
        """Seal encrypted properties in *props* on write (warms the data key first)."""

        if self.cipher is None:
            return props

        await self.cipher.prepare_encrypt()
        return self.cipher.encrypt_mapping(props, record_id=record_id)

    # ....................... #

    async def open(self, props: JsonDict) -> BaseModel:
        """Decrypt *props* (warming first) and decode it into the kind's read model."""

        if self.cipher is None:
            return self.read_codec.decode_mapping(props, trust_source=True)

        await self.cipher.prepare_decrypt([props])
        return self.read_codec.decode_mapping(
            self.cipher.decrypt_mapping(props), trust_source=True
        )


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class GraphCodecs:
    """Resolved per-kind ciphers for a graph module, keyed by node/edge kind name."""

    nodes: dict[str, GraphKindCipher]
    edges: dict[str, GraphKindCipher]

    # ....................... #

    def node(self, kind: str) -> GraphKindCipher:
        return self.nodes[kind]

    def edge(self, kind: str) -> GraphKindCipher:
        return self.edges[kind]


# ....................... #


def _kind_cipher(
    *,
    spec_name: str,
    kind: str,
    read: type[BaseModel],
    encryption: FieldEncryption | None,
    key_field: str | None,
    keyring: FieldCipherPort | None,
    deterministic: DeterministicFieldCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
) -> GraphKindCipher:
    read_codec = default_model_codec(read)

    if encryption is None or encryption.is_empty:
        return GraphKindCipher(read_codec=read_codec, cipher=None)

    if keyring is None:
        raise exc.configuration(
            f"Graph {spec_name!r} kind {kind!r} declares encrypted/searchable properties but "
            "no keyring is wired. Register a CryptoDepsModule or clear the encryption policy.",
            code=_WIRING_CODE,
        )

    if encryption.searchable and deterministic is None:
        raise exc.configuration(
            f"Graph {spec_name!r} kind {kind!r} declares searchable properties but no "
            "deterministic cipher is wired (CryptoDepsModule(deterministic_root=...)).",
            code=_WIRING_CODE,
        )

    if encryption.binds_record_id and key_field is None:
        raise exc.configuration(
            f"Graph {spec_name!r} kind {kind!r} sets FieldEncryption.binds_record_id but has "
            "no key_field (an endpoint-identity edge has no per-edge id). Drop binds_record_id.",
            code=_WIRING_CODE,
        )

    cipher = EncryptingModelCodec(
        inner=read_codec,
        cipher=keyring,
        fields=encryption.encrypted,
        searchable_fields=encryption.searchable,
        deterministic=deterministic,
        tenant_provider=tenant_provider,
        record_id_field=key_field if encryption.binds_record_id else None,
    )

    return GraphKindCipher(read_codec=read_codec, cipher=cipher)


def resolve_graph_codecs(
    spec: GraphModuleSpec,
    *,
    keyring: FieldCipherPort | None,
    deterministic: DeterministicFieldCipherPort | None,
    tenant_provider: Callable[[], TenantIdentity | None],
) -> GraphCodecs:
    """Resolve one :class:`GraphKindCipher` per node and edge kind, fail-closed.

    Declaring an encryption policy on any kind without the matching cipher wired raises rather
    than silently writing / returning plaintext.
    """

    spec_name = str(spec.name)

    nodes = {
        str(n.name): _kind_cipher(
            spec_name=spec_name,
            kind=str(n.name),
            read=n.read,
            encryption=n.encryption,
            key_field=n.key_field,
            keyring=keyring,
            deterministic=deterministic,
            tenant_provider=tenant_provider,
        )
        for n in spec.nodes
    }
    edges = {
        str(e.name): _kind_cipher(
            spec_name=spec_name,
            kind=str(e.name),
            read=e.read,
            encryption=e.encryption,
            key_field=e.key_field,
            keyring=keyring,
            deterministic=deterministic,
            tenant_provider=tenant_provider,
        )
        for e in spec.edges
    }

    return GraphCodecs(nodes=nodes, edges=edges)
