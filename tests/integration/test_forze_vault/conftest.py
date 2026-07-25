"""Pytest configuration for forze_vault integration tests."""

import os
import shutil

import pytest

pytest.importorskip("hvac")
pytest.importorskip("testcontainers.vault")

from testcontainers.vault import VaultContainer

# ----------------------- #

_VAULT_IMAGE = os.environ.get("FORZE_VAULT_IMAGE", "hashicorp/vault:1.16.1")
"""Engine under test. Override to run the same suite against an API-compatible
server — e.g. ``FORZE_VAULT_IMAGE=openbao/openbao:2.2.0`` (OpenBao honors the
``VAULT_DEV_ROOT_TOKEN_ID`` env var and the ``/v1/sys/*`` API this suite uses)."""


@pytest.fixture(scope="session")
def vault_container():
    """Start a dev-mode Vault (or compatible, see ``FORZE_VAULT_IMAGE``) container."""
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Vault integration tests")

    import hvac

    # Testcontainers only sets VAULT_DEV_ROOT_TOKEN_ID; OpenBao's entrypoint reads
    # BAO_DEV_ROOT_TOKEN_ID and would otherwise mint a random root token. Setting
    # both makes the fixture engine-agnostic (the extra var is inert under Vault).
    prepared = VaultContainer(_VAULT_IMAGE)
    prepared.with_env("BAO_DEV_ROOT_TOKEN_ID", prepared.root_token)

    with prepared as container:
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

        try:
            client.sys.enable_secrets_engine(backend_type="transit", path="transit")
        except Exception:
            pass

        yield container, client
