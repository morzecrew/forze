"""Shared search field-encryption resolver: wraps the read codec so in-place search
results decrypt, fail-closed without a keyring."""

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
from forze.base.serialization import default_model_codec
from forze.application.contracts.search import SearchSpec
from forze.application.integrations.crypto import EncryptingModelCodec, Keyring
from forze.application.integrations.search import (
    decrypt_search_rows,
    resolve_search_read_codec_spec,
)
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockKeyManagement

# ----------------------- #


class _Doc(BaseModel):
    id: str
    title: str
    secret: str


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _spec(**kw: object) -> SearchSpec[_Doc]:
    return SearchSpec(name="docs", model_type=_Doc, fields=["title"], **kw)  # type: ignore[arg-type]


# ....................... #


def test_no_encrypted_fields_returns_spec_unchanged() -> None:
    spec = _spec()
    assert (
        resolve_search_read_codec_spec(
            spec, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
        )
        is spec
    )


def test_encrypted_fields_without_keyring_fails_closed() -> None:
    spec = _spec(encryption=FieldEncryption(encrypted=frozenset({"secret"})))

    with pytest.raises(CoreException) as ei:
        resolve_search_read_codec_spec(
            spec, keyring=None, deterministic=None, tenant_provider=lambda: None
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.search.encryption_wiring"


@pytest.mark.asyncio
async def test_wrapped_codec_decrypts_in_place_search_rows() -> None:
    """The resolver yields an encrypting codec that the search read path warms + decodes —
    decrypting the document table's ciphertext out of search results."""

    spec = resolve_search_read_codec_spec(
        _spec(encryption=FieldEncryption(encrypted=frozenset({"secret"}))),
        keyring=_keyring(),
        deterministic=None,
        tenant_provider=lambda: None,
    )
    codec = spec.resolved_read_codec

    # The document gateway would have written this ciphertext; emulate via the same codec.
    await codec.prepare_encrypt()
    row = codec.encode_persistence_mapping(_Doc(id="1", title="t", secret="s3cr3t"))
    assert is_envelope(base64.b64decode(row["secret"]))  # sealed at rest

    # The search read path: warm the decrypt cache, then the synchronous decode.
    await codec.prepare_decrypt([row])
    assert codec.decode_mapping(row) == _Doc(id="1", title="t", secret="s3cr3t")


class _Projection(BaseModel):
    secret: str  # a custom return_type projecting the encrypted field


@pytest.mark.asyncio
async def test_executor_decrypts_rows_once_for_every_decode_path() -> None:
    """The executor decrypts raw rows once and hands back the plain inner codec, so the
    spec model, a custom return_type, and raw field projections all see plaintext."""

    inner = PydanticModelCodec(_Doc)
    codec = EncryptingModelCodec(
        inner=inner,
        cipher=_keyring(),
        fields=frozenset({"secret"}),
        tenant_provider=lambda: None,
    )
    await codec.prepare_encrypt()
    sealed_row = codec.encode_persistence_mapping(_Doc(id="1", title="t", secret="zzz"))
    assert is_envelope(base64.b64decode(sealed_row["secret"]))

    rows, decode_codec = await decrypt_search_rows(codec, [sealed_row])

    # The raw row is now plaintext and the decode codec is the plain inner one.
    assert rows[0]["secret"] == "zzz"
    assert decode_codec is inner

    # Spec model, a custom return_type, and a raw projection all read plaintext.
    assert decode_codec.decode_mapping(rows[0]) == _Doc(id="1", title="t", secret="zzz")
    assert default_model_codec(_Projection).decode_mapping(rows[0]).secret == "zzz"
    assert rows[0]["secret"] == "zzz"  # raw field projection


def test_resolver_wraps_hub_spec_too() -> None:
    """The resolver is generic over the spec type — it also wraps a HubSearchSpec, so
    hub search decrypts its encrypted hub-row fields."""

    from forze.application.contracts.search import HubSearchSpec

    hub = HubSearchSpec(
        name="hub",
        model_type=_Doc,
        members=[_spec()],
        encryption=FieldEncryption(encrypted=frozenset({"secret"})),
    )

    wrapped = resolve_search_read_codec_spec(
        hub, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
    )

    assert isinstance(wrapped, HubSearchSpec)  # same concrete type back
    assert hasattr(wrapped.resolved_read_codec, "prepare_encrypt")  # now encrypting


@pytest.mark.asyncio
async def test_decrypt_rows_is_noop_for_plain_codec() -> None:
    plain = PydanticModelCodec(_Doc)
    row = {"id": "1", "title": "t", "secret": "plain"}

    rows, decode_codec = await decrypt_search_rows(plain, [row])

    assert rows == [row]
    assert decode_codec is plain
