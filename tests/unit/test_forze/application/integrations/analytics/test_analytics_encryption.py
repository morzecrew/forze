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
from forze.application.integrations.crypto import Keyring
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


class _Row(BaseModel):
    id: str
    region: str  # a plaintext dimension we group by
    email: str  # PII: stored, returned, never aggregated


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


def test_binds_record_id_rejected() -> None:
    with pytest.raises(CoreException) as ei:
        resolve_analytics_codecs_spec(
            _spec(
                encryption=FieldEncryption(
                    encrypted=frozenset({"email"}), binds_record_id=True
                )
            ),
            keyring=_keyring(),
            deterministic=None,
            tenant_provider=lambda: None,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "binds_record_id" in str(ei.value)


@pytest.mark.asyncio
async def test_ingest_seals_then_every_read_path_decrypts() -> None:
    spec = _resolved(encrypted=frozenset({"email"}))
    ingest_codec = spec.resolved_ingest_codec
    assert ingest_codec is not None

    # Ingest seals the encrypted column at rest; the dimension stays plaintext.
    sealed = await encode_ingest_payloads(
        ingest_codec, [_Row(id="1", region="eu", email="a@b.co")]
    )
    row = sealed[0]
    assert is_envelope(base64.b64decode(row["email"]))  # sealed
    assert row["region"] == "eu"  # plaintext dimension

    # Every read path decrypts the sealed column once: spec model, custom return_type,
    # and a raw field projection all see plaintext.
    class _Proj(BaseModel):
        email: str

    models = await decrypt_and_shape_rows(
        [dict(row)], read_codec=ingest_codec, read_type=_Row, return_type=None,
        return_fields=None,
    )
    assert models[0] == _Row(id="1", region="eu", email="a@b.co")

    typed = await decrypt_and_shape_rows(
        [dict(row)], read_codec=ingest_codec, read_type=_Row, return_type=_Proj,
        return_fields=None,
    )
    assert typed[0].email == "a@b.co"

    projected = await decrypt_and_shape_rows(
        [dict(row)], read_codec=ingest_codec, read_type=_Row, return_type=None,
        return_fields=["email", "region"],
    )
    assert projected[0] == {"email": "a@b.co", "region": "eu"}


@pytest.mark.asyncio
async def test_decrypt_and_shape_is_noop_for_plain_spec() -> None:
    spec = _spec()
    row = {"id": "1", "region": "eu", "email": "a@b.co"}

    out = await decrypt_and_shape_rows(
        [row], read_codec=spec.resolved_read_codec, read_type=_Row, return_type=None,
        return_fields=None,
    )

    assert out[0] == _Row(id="1", region="eu", email="a@b.co")
