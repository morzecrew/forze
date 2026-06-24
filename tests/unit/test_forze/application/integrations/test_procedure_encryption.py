"""Procedure param encryption: seal declared fields before they are bound into the SQL."""

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
from forze.application.contracts.procedure import ProcedureSpec
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.procedure import resolve_procedure_codecs_spec
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


class _Params(BaseModel):
    secret: str = "x"  # encrypted PII param
    window: str = "2026-01-01"  # plaintext param


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _spec(**enc: object) -> ProcedureSpec[_Params, None]:
    return ProcedureSpec(
        name="recompute",
        params=_Params,
        encryption=FieldEncryption(**enc),  # type: ignore[arg-type]
    )


# ....................... #


def test_no_encryption_returns_spec_unchanged() -> None:
    spec = ProcedureSpec(name="recompute", params=_Params)
    assert (
        resolve_procedure_codecs_spec(
            spec, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
        )
        is spec
    )


def test_empty_encryption_returns_spec_unchanged() -> None:
    spec = _spec()  # FieldEncryption() with no fields → is_empty
    assert (
        resolve_procedure_codecs_spec(
            spec, keyring=_keyring(), deterministic=None, tenant_provider=lambda: None
        )
        is spec
    )


def test_without_keyring_fails_closed() -> None:
    with pytest.raises(CoreException) as ei:
        resolve_procedure_codecs_spec(
            _spec(encrypted=frozenset({"secret"})),
            keyring=None,
            deterministic=None,
            tenant_provider=lambda: None,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.procedures.encryption_wiring"


def test_searchable_without_deterministic_fails_closed() -> None:
    with pytest.raises(CoreException) as ei:
        resolve_procedure_codecs_spec(
            _spec(searchable=frozenset({"secret"})),
            keyring=_keyring(),
            deterministic=None,
            tenant_provider=lambda: None,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.procedures.encryption_wiring"


def test_binds_record_id_rejected_at_construction() -> None:
    with pytest.raises(CoreException) as ei:
        _spec(encrypted=frozenset({"secret"}), binds_record_id=True)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "binds_record_id" in str(ei.value)


@pytest.mark.asyncio
async def test_resolved_codec_seals_encrypted_param() -> None:
    resolved = resolve_procedure_codecs_spec(
        _spec(encrypted=frozenset({"secret"})),
        keyring=_keyring(),
        deterministic=None,
        tenant_provider=lambda: None,
    )

    codec = resolved.resolved_params_codec  # the wrapped encrypting codec
    await codec.prepare_encrypt()  # type: ignore[attr-defined]
    bound = codec.encode_persistence_mapping(_Params(secret="ssn", window="2026-01-01"))

    assert is_envelope(base64.b64decode(bound["secret"]))  # sealed
    assert bound["window"] == "2026-01-01"  # plaintext param untouched
