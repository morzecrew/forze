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
