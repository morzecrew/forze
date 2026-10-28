"""Analytics field encryption: seal on ingest, decrypt out of every read path."""

from __future__ import annotations

import base64

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsSpec
from forze.application.contracts.analytics.specs import AnalyticsQueryDefinition
from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.analytics import (
    decrypt_and_shape_rows,
    encode_ingest_payloads,
    resolve_analytics_codecs_spec,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.integrations.crypto import (
    EncryptingModelCodec,
    Keyring,
    resolve_document_codecs,
)
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import ReadDocument
from forze_mock import MockKeyManagement

# ----------------------- #


class _Row(BaseModel):
    id: str
    region: str  # a plaintext dimension we group by
    email: str  # PII: stored, returned, never aggregated


class _DocRow(ReadDocument):
    region: str
    email: str


class _Params(BaseModel):
    pass


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _spec(**kw: object) -> AnalyticsSpec[_Row, _Row]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        ingest=_Row,
        queries={"all": AnalyticsQueryDefinition(params=_Params)},
        **kw,  # type: ignore[arg-type]
    )


def _resolved(**enc: object) -> AnalyticsSpec[_Row, _Row]:
    return resolve_analytics_codecs_spec(  # type: ignore[return-value]
        _spec(encryption=FieldEncryption(**enc)),  # type: ignore[arg-type]
        keyring=_keyring(),
        deterministic=None,
        tenant_provider=lambda: None,
    )


# ....................... #


def test_no_encryption_returns_spec_unchanged() -> None:
    spec = _spec()
    assert (
        resolve_analytics_codecs_spec(
            spec, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
        )
        is spec
    )


def test_without_keyring_fails_closed() -> None:
    with pytest.raises(CoreException) as ei:
        resolve_analytics_codecs_spec(
            _spec(encryption=FieldEncryption(encrypted=frozenset({"email"}))),
            keyring=None,
            deterministic=None,
            tenant_provider=lambda: None,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.analytics.encryption_wiring"


def test_binds_record_id_rejected_at_construction() -> None:
    # Caught when the spec is built, not deferred to wiring — analytics rows have no id.
    with pytest.raises(CoreException) as ei:
        _spec(encryption=FieldEncryption(encrypted=frozenset({"email"}), binds_record_id=True))

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "binds_record_id" in str(ei.value)


def test_searchable_without_deterministic_fails_closed() -> None:
    with pytest.raises(CoreException) as ei:
        resolve_analytics_codecs_spec(
            _spec(encryption=FieldEncryption(searchable=frozenset({"email"}))),
            keyring=_keyring(),
            deterministic=None,
            tenant_provider=lambda: None,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.analytics.encryption_wiring"


@pytest.mark.asyncio
async def test_ingest_seals_then_every_read_path_decrypts() -> None:
    spec = _resolved(encrypted=frozenset({"email"}))
    ingest_codec = spec.resolved_ingest_codec
    assert ingest_codec is not None

    # Ingest seals the encrypted column at rest; the dimension stays plaintext.
    sealed = await encode_ingest_payloads(ingest_codec, [_Row(id="1", region="eu", email="a@b.co")])
    row = sealed[0]
    assert is_envelope(base64.b64decode(row["email"]))  # sealed
    assert row["region"] == "eu"  # plaintext dimension

    # Every read path decrypts the sealed column once: spec model, custom return_type,
    # and a raw field projection all see plaintext.
    class _Proj(BaseModel):
        email: str

    models = await decrypt_and_shape_rows(
        [dict(row)],
        read_codec=ingest_codec,
        read_type=_Row,
        return_type=None,
        return_fields=None,
    )
    assert models[0] == _Row(id="1", region="eu", email="a@b.co")

    typed = await decrypt_and_shape_rows(
        [dict(row)],
        read_codec=ingest_codec,
        read_type=_Row,
        return_type=_Proj,
        return_fields=None,
    )
    assert typed[0].email == "a@b.co"

    projected = await decrypt_and_shape_rows(
        [dict(row)],
        read_codec=ingest_codec,
        read_type=_Row,
        return_type=None,
        return_fields=["email", "region"],
    )
    assert projected[0] == {"email": "a@b.co", "region": "eu"}


@pytest.mark.asyncio
async def test_decrypt_and_shape_is_noop_for_plain_spec() -> None:
    spec = _spec()
    row = {"id": "1", "region": "eu", "email": "a@b.co"}

    out = await decrypt_and_shape_rows(
        [row],
        read_codec=spec.resolved_read_codec,
        read_type=_Row,
        return_type=None,
        return_fields=None,
    )

    assert out[0] == _Row(id="1", region="eu", email="a@b.co")


def test_spec_reject_plaintext_reaches_analytics_codecs() -> None:
    """The policy's strict mode flows into the wrapped read & ingest codecs."""

    spec = _resolved(encrypted=frozenset({"email"}), reject_plaintext=True)

    read = spec.resolved_read_codec
    assert isinstance(read, EncryptingModelCodec)
    assert read.reject_plaintext is True

    with pytest.raises(CoreException) as ei:
        read.decode_mapping({"id": "1", "region": "eu", "email": "a@b.co"})

    assert ei.value.code == "core.crypto.plaintext_rejected"


# ....................... #
# reads of a document's table (sealed_by)

_POLICY = FieldEncryption(encrypted=frozenset({"email"}))


def _document_sealed_row(keyring: Keyring) -> dict[str, object]:
    """A row as the document port stores it: the email sealed under the document's name."""

    codecs = resolve_document_codecs(
        DocumentSpec(name="events_doc", read=_DocRow, encryption=_POLICY).resolved_codecs,
        spec_name="events_doc",
        encryption=_POLICY,
        keyring=keyring,
        deterministic=None,
        tenant_provider=lambda: None,
        integration="test",
        code="test.encryption_wiring",
    )
    read = codecs.read
    assert isinstance(read, EncryptingModelCodec)
    return read.encrypt_mapping({"id": "1", "region": "eu", "email": "a@b.co"})


def _query_only(**kw: object) -> AnalyticsSpec[_Row, _Row]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"all": AnalyticsQueryDefinition(params=_Params)},
        encryption=_POLICY,
        **kw,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("sealed_by", ["events_doc", None])
async def test_a_query_opens_a_documents_rows_only_when_sealed_by_names_it(
    sealed_by: str | None,
) -> None:
    """Sealed by the real document codec, not by the analytics codec: naming the document
    opens the row; without the name it fails authentication, never opened under another."""

    keyring = _keyring()
    await keyring.warm(None)
    row = _document_sealed_row(keyring)
    assert is_envelope(base64.b64decode(str(row["email"])))

    read = resolve_analytics_codecs_spec(
        _query_only(sealed_by=sealed_by),
        keyring=keyring,
        deterministic=None,
        tenant_provider=lambda: None,
    ).resolved_read_codec
    await read.prepare_decrypt([row])  # type: ignore[attr-defined]

    if sealed_by is not None:
        assert read.decode_mapping(row) == _Row(id="1", region="eu", email="a@b.co")
        return

    with pytest.raises(CoreException) as ei:
        read.decode_mapping(row)

    assert ei.value.code == "core.crypto.aead_auth_failed"


def test_sealed_by_with_ingest_is_refused() -> None:
    """Ingest seals under the analytics label, which a sealed_by read codec cannot open."""

    with pytest.raises(CoreException) as ei:
        _query_only(sealed_by="events_doc", ingest=_Row)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "sealed_by and ingest" in str(ei.value)


def test_empty_sealed_by_is_rejected() -> None:
    with pytest.raises(ValueError, match="sealed_by"):
        _query_only(sealed_by="")
