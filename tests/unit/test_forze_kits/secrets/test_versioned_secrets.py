"""Versioned reads + control-plane writes across the stdlib-backed secrets adapters."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType

import pytest

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsAdminDepKey,
    SecretsDepKey,
    content_secret_version,
)
from forze.base.exceptions import CoreException
from forze_kits.adapters.secrets import (
    DirectorySecrets,
    EnvSecrets,
    MappingSecrets,
    SecretsDepsModule,
)

# ----------------------- #

_REF = SecretRef("db/dsn")


class TestMappingVersionedSecrets:
    async def test_versioned_read_matches_content_hash(self) -> None:
        backend = MappingSecrets(data={"db/dsn": "dsn-1"})

        value = await backend.resolve_versioned(_REF)
        assert value.text == "dsn-1"
        assert value.version == content_secret_version("dsn-1")
        assert await backend.current_version(_REF) == value.version

    async def test_put_changes_value_and_version(self) -> None:
        backend = MappingSecrets(data={"db/dsn": "dsn-1"})
        before = await backend.current_version(_REF)

        version = await backend.put(_REF, "dsn-2")

        assert await backend.resolve_str(_REF) == "dsn-2"
        assert version != before
        assert await backend.current_version(_REF) == version

    async def test_capabilities_reflect_mutability(self) -> None:
        mutable = MappingSecrets(data={"db/dsn": "x"})
        frozen = MappingSecrets(data=MappingProxyType({"db/dsn": "x"}))

        assert mutable.secrets_capabilities.writes
        assert not frozen.secrets_capabilities.writes

    async def test_put_on_read_only_mapping_fails_closed(self) -> None:
        backend = MappingSecrets(data=MappingProxyType({"db/dsn": "x"}))

        with pytest.raises(CoreException, match="not supported") as excinfo:
            await backend.put(_REF, "y")

        assert excinfo.value.code == "secrets_feature_unsupported"


class TestEnvVersionedSecrets:
    async def test_versioned_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("APP_DSN", "dsn-env")
        backend = EnvSecrets()

        value = await backend.resolve_versioned(SecretRef("APP_DSN"))

        assert value.text == "dsn-env"
        assert value.version == content_secret_version("dsn-env")

    async def test_capabilities_are_read_only(self) -> None:
        caps = EnvSecrets().secrets_capabilities

        assert caps.versioned_reads
        assert not caps.writes
        assert not caps.change_feed


class TestDirectoryVersionedSecrets:
    async def test_versioned_read_tracks_file_content(self, tmp_path) -> None:
        (tmp_path / "dsn").write_text("dsn-a", encoding="utf-8")
        backend = DirectorySecrets(root=tmp_path)
        ref = SecretRef("dsn")

        first = await backend.current_version(ref)
        (tmp_path / "dsn").write_text("dsn-b", encoding="utf-8")

        assert await backend.current_version(ref) != first

    async def test_capabilities_declare_native_change_source(self) -> None:
        caps = DirectorySecrets(root=Path()).secrets_capabilities

        assert caps.versioned_reads
        assert caps.change_feed
        assert not caps.writes


class TestSecretsDepsModuleAdmin:
    def test_admin_key_registered_only_when_set(self) -> None:
        backend = MappingSecrets(data={"db/dsn": "x"})

        plain = SecretsDepsModule(secrets=backend)()
        assert SecretsAdminDepKey not in plain.plain_deps

        wired = SecretsDepsModule(secrets=backend, secrets_admin=backend)()
        assert wired.plain_deps[SecretsDepKey] is backend
        assert wired.plain_deps[SecretsAdminDepKey] is backend
