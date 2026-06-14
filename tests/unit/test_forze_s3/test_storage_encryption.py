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
