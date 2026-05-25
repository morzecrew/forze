"""Unit tests for :class:`~forze_secrets.execution.SecretsDepsModule`."""

from forze.application.contracts.secrets import SecretsDepKey
from forze_secrets import MappingSecrets, SecretsDepsModule

# ----------------------- #


def test_secrets_deps_module_registers_port() -> None:
    backend = MappingSecrets({"k": "v"})
    deps = SecretsDepsModule(secrets=backend)()

    assert deps.plain_deps[SecretsDepKey] is backend
