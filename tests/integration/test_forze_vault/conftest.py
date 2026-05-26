"""Pytest configuration for forze_vault integration tests."""

import shutil

import pytest

pytest.importorskip("hvac")
pytest.importorskip("testcontainers.vault")

from testcontainers.vault import VaultContainer

# ----------------------- #


@pytest.fixture(scope="session")
def vault_container():
    """Start a dev-mode Vault container with KV v2 enabled."""
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Vault integration tests")

    import hvac

    with VaultContainer("hashicorp/vault:1.16.1") as container:
        client = hvac.Client(
            url=container.get_connection_url(),
            token=container.root_token,
        )
        try:
            client.sys.enable_secrets_engine(
                backend_type="kv",
                path="secret",
                options={"version": "2"},
            )
        except Exception:
            pass

        yield container, client
