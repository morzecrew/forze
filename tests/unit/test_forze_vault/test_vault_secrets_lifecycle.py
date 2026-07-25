"""Vault lifecycle adapters: versioned reads, admin put, dynamic leases, deps wiring."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsAdminDepKey,
    SecretsDepKey,
    SecretsLeaseDepKey,
)
from forze.base.exceptions import CoreException

pytest.importorskip("hvac")

from forze_vault import (
    VaultClient,
    VaultDepsModule,
    VaultDynamicSecrets,
    VaultKvSecrets,
)

# ----------------------- #

_REF = SecretRef("tenants/t1/dsn")


def _client() -> MagicMock:
    client = MagicMock(spec=VaultClient)
    client.read_kv_data_versioned = AsyncMock(return_value=({"value": "dsn-7"}, 7))
    client.read_kv_metadata = AsyncMock(return_value={"current_version": 7})
    client.write_kv_data = AsyncMock(return_value=8)
    client.db_generate_credentials = AsyncMock(
        return_value={
            "lease_id": "database/creds/app/abc",
            "lease_duration": 300,
            "renewable": True,
            "data": {"username": "v-app-1", "password": "pw"},
        }
    )
    client.renew_lease = AsyncMock(return_value=120)
    client.revoke_lease = AsyncMock(return_value=None)

    return client


class TestVaultVersionedSecrets:
    async def test_resolve_versioned_carries_the_native_version(self) -> None:
        adapter = VaultKvSecrets(client=_client())

        value = await adapter.resolve_versioned(_REF)

        assert value.text == "dsn-7"
        assert value.version.token == "7"

    async def test_current_version_reads_metadata_only(self) -> None:
        client = _client()
        adapter = VaultKvSecrets(client=client)

        version = await adapter.current_version(_REF)

        assert version.token == "7"
        client.read_kv_metadata.assert_awaited_once_with("tenants/t1/dsn")
        client.read_kv_data.assert_not_awaited()

    async def test_current_version_rejects_malformed_metadata(self) -> None:
        client = _client()
        client.read_kv_metadata = AsyncMock(return_value={"oops": True})
        adapter = VaultKvSecrets(client=client)

        with pytest.raises(CoreException, match="current_version"):
            await adapter.current_version(_REF)

    async def test_put_round_trips_the_single_value_shape(self) -> None:
        client = _client()
        adapter = VaultKvSecrets(client=client)

        version = await adapter.put(_REF, "new-dsn")

        assert version.token == "8"
        client.write_kv_data.assert_awaited_once_with("tenants/t1/dsn", {"value": "new-dsn"})

    async def test_capabilities_declare_native_versions_and_writes(self) -> None:
        caps = VaultKvSecrets(client=_client()).secrets_capabilities

        assert caps.versioned_reads
        assert caps.native_versions
        assert caps.writes
        assert not caps.change_feed


class TestVaultDynamicSecrets:
    async def test_issue_shapes_a_leased_secret(self) -> None:
        adapter = VaultDynamicSecrets(client=_client())

        leased = await adapter.issue(SecretRef("app-role"))

        assert leased.lease_id == "database/creds/app/abc"
        assert leased.ttl == timedelta(seconds=300)
        assert leased.renewable
        assert '"username":"v-app-1"' in leased.text

    async def test_issue_rejects_malformed_payload(self) -> None:
        client = _client()
        client.db_generate_credentials = AsyncMock(return_value={"data": {}})
        adapter = VaultDynamicSecrets(client=client)

        with pytest.raises(CoreException, match="unexpected payload"):
            await adapter.issue(SecretRef("app-role"))

    def test_lease_manager_accepts_the_adapter(self) -> None:
        """The fail-closed capability gate must pass for the real adapter, not just
        test doubles — this is exactly what a mock-only suite would never catch."""

        from forze_kits.integrations.secrets import SecretsLeaseManager

        async def _on_credential(ref: SecretRef, leased: object) -> None:  # pragma: no cover
            pass

        manager = SecretsLeaseManager(
            dynamic=VaultDynamicSecrets(client=_client()),
            roles=(SecretRef("app-role"),),
            on_credential=_on_credential,  # type: ignore[arg-type]
        )

        assert manager.dynamic.secrets_capabilities.dynamic_credentials  # type: ignore[attr-defined]

    async def test_renew_and_revoke_pass_through(self) -> None:
        client = _client()
        adapter = VaultDynamicSecrets(client=client)

        granted = await adapter.renew("lease-1", timedelta(seconds=300))

        assert granted == timedelta(seconds=120)
        client.renew_lease.assert_awaited_once_with("lease-1", 300)

        await adapter.revoke("lease-1")
        client.revoke_lease.assert_awaited_once_with("lease-1")


class TestVaultDepsWiring:
    def test_default_adapter_serves_the_admin_key_too(self) -> None:
        client = _client()

        deps = VaultDepsModule(client=client)()

        assert deps.plain_deps[SecretsAdminDepKey] is deps.plain_deps[SecretsDepKey]

    def test_custom_read_only_secrets_leaves_admin_unregistered(self) -> None:
        class _CustomSecrets:
            async def resolve_str(self, ref: SecretRef) -> str:  # pragma: no cover
                return "x"

            async def exists(self, ref: SecretRef) -> bool:  # pragma: no cover
                return True

        deps = VaultDepsModule(client=_client(), secrets=_CustomSecrets())()

        assert SecretsAdminDepKey not in deps.plain_deps

    def test_dynamic_secrets_registered_when_set(self) -> None:
        client = _client()
        dynamic = VaultDynamicSecrets(client=client)

        deps = VaultDepsModule(client=client, dynamic_secrets=dynamic)()

        assert deps.plain_deps[SecretsLeaseDepKey] is dynamic
