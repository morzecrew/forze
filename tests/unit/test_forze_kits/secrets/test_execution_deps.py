"""Unit tests for :class:`~forze_kits.adapters.secrets.SecretsDepsModule`."""

from forze.application.contracts.secrets import SecretsDepKey
from forze_kits.adapters.secrets import MappingSecrets, SecretsDepsModule

# ----------------------- #


def test_secrets_deps_module_registers_port() -> None:
    backend = MappingSecrets(data={"k": "v"})
    deps = SecretsDepsModule(secrets=backend)()

    assert deps.plain_deps[SecretsDepKey] is backend
