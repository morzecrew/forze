"""Unit tests for S3 storage encryption wiring (config flag + fail-closed floor)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import CryptoDepsModule
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement
from forze_s3.execution.deps import S3DepsModule
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.kernel.client import S3ClientPort
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _crypto() -> CryptoDepsModule:
    return CryptoDepsModule(
        kms=MockKeyManagement(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


# ....................... #


def test_encrypt_route_injects_cipher() -> None:
    client = MagicMock(spec=S3ClientPort)
    ctx = context_from_modules(
        _crypto(),
        S3DepsModule(
            client=client,
            storages={"docs": S3StorageConfig(bucket="b", encrypt=True)},
        ),
    )

    adapter = ctx.storage.query(StorageSpec(name="docs"))

    assert adapter.cipher is not None


# ....................... #


def test_plain_route_has_no_cipher() -> None:
    client = MagicMock(spec=S3ClientPort)
    ctx = context_from_modules(
        _crypto(),
        S3DepsModule(
            client=client,
            storages={"docs": S3StorageConfig(bucket="b")},
        ),
    )

    adapter = ctx.storage.query(StorageSpec(name="docs"))

    assert adapter.cipher is None


# ....................... #


def test_required_encryption_fails_closed_on_plain_route() -> None:
    client = MagicMock(spec=S3ClientPort)

    with pytest.raises(CoreException) as excinfo:
        S3DepsModule(
            client=client,
            storages={"docs": S3StorageConfig(bucket="b")},  # encrypt defaults False
            required_encryption="envelope",
        )

    assert excinfo.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


def test_required_encryption_satisfied_by_encrypt_route() -> None:
    client = MagicMock(spec=S3ClientPort)

    # Should not raise.
    S3DepsModule(
        client=client,
        storages={"docs": S3StorageConfig(bucket="b", encrypt=True)},
        required_encryption="envelope",
    )


# ....................... #


def _ctx(*, encrypt: bool):  # type: ignore[no-untyped-def]
    return context_from_modules(
        _crypto(),
        S3DepsModule(
            client=MagicMock(spec=S3ClientPort),
            storages={"docs": S3StorageConfig(bucket="b", encrypt=encrypt)},
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
