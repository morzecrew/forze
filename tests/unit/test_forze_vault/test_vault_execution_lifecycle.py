"""Unit tests for Vault execution lifecycle."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("hvac")

from forze.application.execution import ExecutionContext
from forze_vault.execution import VaultClientDepKey, vault_lifecycle_step
from forze_vault.execution.lifecycle import VaultShutdownHook, VaultStartupHook
from forze_vault.kernel.client import VaultClient

# ----------------------- #


@pytest.mark.asyncio
async def test_vault_startup_hook_initializes_client() -> None:
    client = MagicMock(spec=VaultClient)
    client.initialize = AsyncMock()

    ctx = MagicMock(spec=ExecutionContext)
    ctx.deps.provide = MagicMock(return_value=client)

    await VaultStartupHook()(ctx)

    ctx.deps.provide.assert_called_once_with(VaultClientDepKey)
    client.initialize.assert_awaited_once()


def test_vault_lifecycle_step_exposes_hooks() -> None:
    step = vault_lifecycle_step(name="v1")
    assert step.id == "v1"
    assert isinstance(step.startup, VaultStartupHook)
    assert isinstance(step.shutdown, VaultShutdownHook)


@pytest.mark.asyncio
async def test_vault_shutdown_hook_closes_client() -> None:
    client = MagicMock(spec=VaultClient)
    client.close = AsyncMock()

    ctx = MagicMock(spec=ExecutionContext)
    ctx.deps.provide = MagicMock(return_value=client)

    await VaultShutdownHook()(ctx)

    client.close.assert_awaited_once()
