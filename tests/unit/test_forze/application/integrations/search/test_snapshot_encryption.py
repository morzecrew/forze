"""Search result snapshot at-rest encryption: seal stored record models, open on replay."""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.search import (
    SearchResultSnapshot,
    resolve_snapshot_cipher,
)
from forze.application.integrations.search.snapshot import _SEALED_PREFIX
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


def test_resolve_snapshot_cipher_fail_closed_without_keyring() -> None:
    # An encrypted route must not silently snapshot plaintext when no keyring is wired.
    with pytest.raises(CoreException) as ei:
        resolve_snapshot_cipher(encrypted=True, keyring=None)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.search.snapshot_encryption_wiring"


def test_resolve_snapshot_cipher_none_when_not_encrypted() -> None:
    assert resolve_snapshot_cipher(encrypted=False, keyring=None) is None


# ----------------------- #


class _Hit(BaseModel):
    id: str
    ssn: str


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _snap(cipher: Keyring | None) -> SearchResultSnapshot:
    # store is unused by the seal/open helpers under test.
    return SearchResultSnapshot(
        store=object(),  # type: ignore[arg-type]
        cipher=cipher,
        cipher_tenant=lambda: None,
    )


# ....................... #


@pytest.mark.asyncio
async def test_seal_hides_models_and_open_round_trips() -> None:
    snap = _snap(_keyring())
    keys = [SearchResultSnapshot.result_record_key_string(_Hit(id="1", ssn="secret"))]

    sealed = await snap._seal_ids(keys, run_id="run-1")

    # At rest the record is a sealed token, not the model JSON.
    assert sealed[0].startswith(_SEALED_PREFIX)
    assert "secret" not in sealed[0]

    opened = await snap._open_ids(sealed, run_id="run-1")
    assert opened == keys
    assert SearchResultSnapshot.hydrate_result_record_key(opened[0], _Hit).ssn == "secret"


@pytest.mark.asyncio
async def test_no_cipher_is_plaintext_passthrough() -> None:
    snap = _snap(None)
    keys = ['{"id":"1","ssn":"x"}']

    assert await snap._seal_ids(keys, run_id="r") == keys
    assert await snap._open_ids(keys, run_id="r") == keys


@pytest.mark.asyncio
async def test_legacy_plaintext_keys_still_open() -> None:
    # A run written before encryption was enabled (plaintext JSON keys, no sentinel).
    snap = _snap(_keyring())
    legacy = [json.dumps({"id": "1", "ssn": "x"})]

    assert await snap._open_ids(legacy, run_id="r") == legacy


@pytest.mark.asyncio
async def test_open_fails_closed_when_sealed_but_no_cipher() -> None:
    sealed = await _snap(_keyring())._seal_ids(['{"id":"1"}'], run_id="r")

    with pytest.raises(CoreException) as ei:
        await _snap(None)._open_ids(sealed, run_id="r")

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.search.snapshot_encryption_wiring"


@pytest.mark.asyncio
async def test_aad_binds_run_id() -> None:
    snap = _snap(_keyring())
    sealed = await snap._seal_ids(['{"id":"1"}'], run_id="run-A")

    # The same sealed token cannot be opened under a different run id (AAD mismatch).
    with pytest.raises(CoreException):
        await snap._open_ids(sealed, run_id="run-B")
