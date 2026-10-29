"""Unit tests for GCS storage encryption wiring (mirrors S3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import CryptoDepsModule
from forze.base.exceptions import CoreException, ExceptionKind
from forze_gcs.execution.deps import GCSDepsModule
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.kernel.client import GCSClientPort
from forze_mock import MockKeyManagement
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _crypto() -> CryptoDepsModule:
    return CryptoDepsModule(
        kms=MockKeyManagement(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


# ....................... #


def test_encrypt_route_injects_cipher() -> None:
    client = MagicMock(spec=GCSClientPort)
    ctx = context_from_modules(
        _crypto(),
        GCSDepsModule(
            client=client,
            storages={"docs": GCSStorageConfig(bucket="b", encrypt=True)},
        ),
    )

    assert ctx.storage.query(StorageSpec(name="docs")).cipher is not None


# ....................... #


def test_required_encryption_fails_closed_on_plain_route() -> None:
    client = MagicMock(spec=GCSClientPort)

    with pytest.raises(CoreException) as excinfo:
        GCSDepsModule(
            client=client,
            storages={"docs": GCSStorageConfig(bucket="b")},
            required_encryption="envelope",
        )

    assert excinfo.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


def _ctx(*, encrypt: bool):  # type: ignore[no-untyped-def]
    return context_from_modules(
        _crypto(),
        GCSDepsModule(
            client=MagicMock(spec=GCSClientPort),
            storages={"docs": GCSStorageConfig(bucket="b", encrypt=encrypt)},
        ),
    )


@pytest.mark.parametrize("accessor", ["query", "command", "uploads"])
def test_a_direct_transfer_spec_is_refused_on_an_encrypt_route(accessor: str) -> None:
    ctx = _ctx(encrypt=True)

    with pytest.raises(CoreException) as excinfo:
        getattr(ctx.storage, accessor)(StorageSpec(name="docs", direct_transfer=True))

    assert excinfo.value.kind is ExceptionKind.CONFIGURATION
    assert excinfo.value.code == "storage_direct_transfer_encrypted"


def test_a_direct_transfer_spec_resolves_on_a_plain_route() -> None:
    ctx = _ctx(encrypt=False)

    assert ctx.storage.query(StorageSpec(name="docs", direct_transfer=True)) is not None


def test_a_plain_spec_still_resolves_on_an_encrypt_route() -> None:
    assert _ctx(encrypt=True).storage.query(StorageSpec(name="docs")).cipher is not None
