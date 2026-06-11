"""Integration tests for Vault token renewal and health against a real container."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze_vault.kernel.client import VaultClient, VaultConfig

# ----------------------- #

@pytest.mark.integration
@pytest.mark.asyncio
async def test_renewal_cycle_with_short_ttl_token(vault_container) -> None:
    container, hvac_client = vault_container
    url = container.get_connection_url()

    created = hvac_client.auth.token.create(
        ttl="10s",
        renewable=True,
        policies=["default"],
    )
    token = created["auth"]["client_token"]
    accessor = created["auth"]["accessor"]

    config = VaultConfig(
        url=url,
        token=token,
        verify=False,
        renew_token=True,
        renew_interval=timedelta(seconds=1),
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        assert client._renew_task is not None

        await asyncio.sleep(2.5)

        lookup = hvac_client.auth.token.lookup_accessor(accessor)
        data = lookup["data"]

        # The background task renewed the lease at least once.
        assert data.get("last_renewal_time") or data.get("last_renewal")
        # And the TTL was extended past what 2.5s of decay would leave.
        assert data["ttl"] > 8

    finally:
        await client.close()

    assert client._renew_task is None

@pytest.mark.integration
@pytest.mark.asyncio
async def test_non_renewable_root_token_skips_renewal(vault_container) -> None:
    container, _hvac_client = vault_container
    url = container.get_connection_url()

    config = VaultConfig(
        url=url,
        token=container.root_token,
        verify=False,
        renew_token=True,
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        # Root tokens are not renewable: warning + no background task.
        assert client._renew_task is None

    finally:
        await client.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_against_real_vault(vault_container) -> None:
    container, _hvac_client = vault_container

    config = VaultConfig(
        url=container.get_connection_url(),
        token=container.root_token,
        verify=False,
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        assert await client.health() == ("ok", True)

    finally:
        await client.close()
