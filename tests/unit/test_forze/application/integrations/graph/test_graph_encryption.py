"""Graph field encryption: seal node/edge properties on write, decrypt on read."""

from __future__ import annotations

import base64

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.graph import (
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
)
from forze.application.contracts.graph.types import GraphEdgeDirectionality
from forze.application.contracts.graph.value_objects import GraphEdgeEndpoint
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.graph import resolve_graph_codecs
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


class _Person(BaseModel):
    id: str
    name: str
    ssn: str  # confidential: sealed at rest, decrypted on read


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _module(node_encryption: FieldEncryption | None) -> GraphModuleSpec:
    return GraphModuleSpec(
        name="people",
        nodes=(
            GraphNodeSpec(name="Person", read=_Person, encryption=node_encryption),
        ),
        edges=(),
    )


def _resolve(node_encryption: FieldEncryption | None, *, keyring: Keyring | None):
    return resolve_graph_codecs(
        _module(node_encryption),
        keyring=keyring,
        deterministic=None,
        tenant_provider=lambda: None,
    )


# ....................... #


def test_no_encryption_yields_plaintext_kind() -> None:
    codecs = _resolve(None, keyring=_keyring())
    assert codecs.node("Person").cipher is None


def test_without_keyring_fails_closed() -> None:
    with pytest.raises(CoreException) as ei:
        _resolve(FieldEncryption(encrypted=frozenset({"ssn"})), keyring=None)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.graph.encryption_wiring"


def test_binds_record_id_without_key_field_rejected() -> None:
    # An endpoint-identity edge has key_field=None, so binds_record_id is unsupported.
    module = GraphModuleSpec(
        name="links",
        nodes=(GraphNodeSpec(name="Person", read=_Person),),
        edges=(
            GraphEdgeSpec(
                name="KNOWS",
                read=_Person,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="Person", to_kind="Person"),),
                directionality=GraphEdgeDirectionality.SYMMETRIC,
                encryption=FieldEncryption(
                    encrypted=frozenset({"ssn"}), binds_record_id=True
                ),
            ),
        ),
    )

    with pytest.raises(CoreException) as ei:
        resolve_graph_codecs(
            module, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
        )

    assert "binds_record_id" in str(ei.value)


def test_binds_record_id_on_endpoint_edge_with_key_field_rejected() -> None:
    # identity="endpoints" is addressed by its endpoints, not a per-edge key — so even with a
    # stray key_field set, binds_record_id must be rejected (the key is not the edge identity).
    module = GraphModuleSpec(
        name="links",
        nodes=(GraphNodeSpec(name="Person", read=_Person),),
        edges=(
            GraphEdgeSpec(
                name="KNOWS",
                read=_Person,
                identity="endpoints",
                key_field="id",
                endpoints=(GraphEdgeEndpoint(from_kind="Person", to_kind="Person"),),
                directionality=GraphEdgeDirectionality.SYMMETRIC,
                encryption=FieldEncryption(
                    encrypted=frozenset({"ssn"}), binds_record_id=True
                ),
            ),
        ),
    )

    with pytest.raises(CoreException) as ei:
        resolve_graph_codecs(
            module, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
        )

    assert ei.value.code == "core.graph.encryption_wiring"


@pytest.mark.asyncio
async def test_seal_on_write_then_decrypt_on_read() -> None:
    codecs = _resolve(
        FieldEncryption(encrypted=frozenset({"ssn"})), keyring=_keyring()
    )
    cipher = codecs.node("Person")

    sealed = await cipher.seal(
        {"id": "1", "name": "Ann", "ssn": "123-45-6789"}, record_id=None
    )
    assert is_envelope(base64.b64decode(sealed["ssn"]))  # confidential, sealed
    assert sealed["name"] == "Ann"  # plaintext property stays queryable

    model = await cipher.open(sealed)
    assert model == _Person(id="1", name="Ann", ssn="123-45-6789")


@pytest.mark.asyncio
async def test_plaintext_kind_open_is_plain_decode() -> None:
    cipher = _resolve(None, keyring=_keyring()).node("Person")
    model = await cipher.open({"id": "1", "name": "Ann", "ssn": "x"})
    assert model == _Person(id="1", name="Ann", ssn="x")
