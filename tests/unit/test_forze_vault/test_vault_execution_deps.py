"""Unit tests for Vault deps module."""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("hvac")

from forze.application.contracts.secrets import SecretsDepKey
from forze_vault.adapters import VaultKvSecrets
from forze_vault.execution import VaultClientDepKey, VaultDepsModule
from forze_vault.kernel.platform import VaultClient

# ----------------------- #


def test_vault_deps_module_registers_client_and_secrets() -> None:
    client = MagicMock(spec=VaultClient)
    deps = VaultDepsModule(client=client)()

    assert deps.plain_deps[VaultClientDepKey] is client
    assert isinstance(deps.plain_deps[SecretsDepKey], VaultKvSecrets)


def test_vault_deps_module_custom_secrets_adapter() -> None:
    client = MagicMock(spec=VaultClient)
    custom = MagicMock()
    deps = VaultDepsModule(client=client, secrets=custom)()

    assert deps.plain_deps[SecretsDepKey] is custom
