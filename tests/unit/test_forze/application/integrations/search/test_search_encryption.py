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
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.application.integrations.crypto import (
    DeterministicFieldCipher,
    EncryptingModelCodec,
    Keyring,
    resolve_document_codecs,
)
from forze.application.integrations.search import (
    assert_search_encryption_parity,
    decrypt_search_rows,
    refuse_index_sealed_by,
    resolve_search_read_codec_spec,
)
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec, default_model_codec
from forze.domain.models import ReadDocument
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


class _DocRead(ReadDocument):
    title: str
    secret: str


_POLICY = FieldEncryption(encrypted=frozenset({"secret"}))


def _document_sealed_row(keyring: Keyring) -> dict[str, object]:
    """A row as the document port stores it: the secret sealed under the document's name."""

    document = resolve_document_codecs(
        DocumentSpec(name="docs", read=_DocRead, encryption=_POLICY).resolved_codecs,
        spec_name="docs",
        encryption=_POLICY,
        keyring=keyring,
        deterministic=None,
        tenant_provider=lambda: None,
        integration="test",
        code="test.encryption_wiring",
    ).read
    assert isinstance(document, EncryptingModelCodec)
    return document.encrypt_mapping({"id": "1", "title": "t", "secret": "s3cr3t"})


@pytest.mark.asyncio
@pytest.mark.parametrize("hub", [False, True])
@pytest.mark.parametrize("sealed_by", ["docs", None])
async def test_in_place_search_opens_a_documents_rows_only_when_sealed_by_names_it(
    sealed_by: str | None, hub: bool
) -> None:
    """Sealed by the real document codec, not by the search codec: naming the document opens
    the row (a hub names its hub relation's); without it the row fails authentication."""

    keyring = _keyring()
    await keyring.warm(None)
    row = _document_sealed_row(keyring)
    spec: SearchSpec[_Doc] | HubSearchSpec[_Doc] = (
        HubSearchSpec(
            name="hub", model_type=_Doc, members=[_spec()], encryption=_POLICY, sealed_by=sealed_by
        )
        if hub
        else _spec(encryption=_POLICY, sealed_by=sealed_by)
    )

    codec = resolve_search_read_codec_spec(
        spec, keyring=keyring, deterministic=None, tenant_provider=lambda: None
    ).resolved_read_codec
    await codec.prepare_decrypt([row])  # type: ignore[attr-defined]

    if sealed_by is not None:
        assert codec.decode_mapping(row) == _Doc(id="1", title="t", secret="s3cr3t")
        return

    with pytest.raises(CoreException) as ei:
        codec.decode_mapping(row)

    assert ei.value.code == "core.crypto.aead_auth_failed"


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_sealed_by_is_rejected(blank: str) -> None:
    with pytest.raises(CoreException, match="sealed_by") as ei:
        _spec(encryption=_POLICY, sealed_by=blank)

    assert ei.value.kind is ExceptionKind.CONFIGURATION


@pytest.mark.parametrize("member", [None, "docs"])
def test_hub_members_leave_sealed_by_unset_or_equal_to_the_hubs(member: str | None) -> None:
    """The hub's sealed_by governs every hub row it decodes, on every backend."""

    def hub(member_sealed_by: str | None) -> HubSearchSpec[_Doc]:
        return HubSearchSpec(
            name="hub",
            model_type=_Doc,
            members=[_spec(encryption=_POLICY, sealed_by=member_sealed_by)],
            encryption=_POLICY,
            sealed_by="docs",
        )

    assert hub(member).sealed_by == "docs"

    with pytest.raises(CoreException, match="name another document") as ei:
        hub("other")

    assert ei.value.kind is ExceptionKind.CONFIGURATION


def test_an_index_that_seals_its_own_fields_refuses_sealed_by() -> None:
    refuse_index_sealed_by(_spec(encryption=_POLICY), backend="Test")

    with pytest.raises(CoreException) as ei:
        refuse_index_sealed_by(_spec(encryption=_POLICY, sealed_by="docs"), backend="Test")

    assert ei.value.code == "core.search.encryption_wiring"


def test_parity_refuses_sealed_by_naming_another_document() -> None:
    document = DocumentSpec(name="docs", read=_DocRead, encryption=_POLICY)

    assert_search_encryption_parity(
        document=document, search=_spec(encryption=_POLICY, sealed_by="docs")
    )

    with pytest.raises(CoreException) as ei:
        assert_search_encryption_parity(
            document=document, search=_spec(encryption=_POLICY, sealed_by="other")
        )

    assert ei.value.code == "search_encryption_parity_mismatch"


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


def test_spec_reject_plaintext_reaches_search_codec() -> None:
    """The policy's strict mode flows into the wrapped search read codec, so a
    tampered / unauthentic value in a searchable slot raises instead of being
    silently passed through as legacy plaintext."""

    spec = resolve_search_read_codec_spec(
        _spec(encryption=FieldEncryption(searchable=frozenset({"secret"}), reject_plaintext=True)),
        keyring=_keyring(),
        deterministic=DeterministicFieldCipher(root=b"a-stable-root-secret-32-bytes!!!"),
        tenant_provider=lambda: None,
    )
    codec = spec.resolved_read_codec

    assert isinstance(codec, EncryptingModelCodec)
    assert codec.reject_plaintext is True

    # Valid base64 but not our ciphertext: post-migration this is a tamper signal
    # and must surface, not decode as a legacy plaintext row.
    forged = base64.b64encode(b"not-our-ciphertext-at-all").decode()
    with pytest.raises(CoreException) as ei:
        codec.decode_mapping({"id": "1", "title": "t", "secret": forged})

    assert ei.value.code == "core.crypto.plaintext_rejected"


def test_spec_default_keeps_search_codec_lenient() -> None:
    spec = resolve_search_read_codec_spec(
        _spec(encryption=FieldEncryption(encrypted=frozenset({"secret"}))),
        keyring=_keyring(),
        deterministic=None,
        tenant_provider=lambda: None,
    )
    codec = spec.resolved_read_codec

    assert isinstance(codec, EncryptingModelCodec)
    assert codec.reject_plaintext is False


@pytest.mark.asyncio
async def test_decrypt_rows_is_noop_for_plain_codec() -> None:
    plain = PydanticModelCodec(_Doc)
    row = {"id": "1", "title": "t", "secret": "plain"}

    rows, decode_codec = await decrypt_search_rows(plain, [row])

    assert rows == [row]
    assert decode_codec is plain
