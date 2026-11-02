"""A Keycloak container with an imported realm, addressed from the test process only.

The image pin is shared with the Lakekeeper stack (:mod:`tests.support.lakekeeper`), which
runs Keycloak on a Docker network so other containers reach it. This helper is the simpler
case: one container, reached on the published port at the container host, so the issuer is
``http://localhost:<port>/realms/<realm>`` — a loopback address, which the OIDC verifiers
accept over plain http. The stack skips when the container host is not loopback.

**Exposure.** The realm's client secrets are ``${ENV}`` placeholders Keycloak resolves from
its own environment, so the realm file on disk holds none; the admin password is generated per
run. Keycloak publishes its port on all host interfaces for the context's lifetime.
"""

from __future__ import annotations

import json
import secrets
import tempfile
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from ipaddress import ip_address
from pathlib import Path
from typing import Any

import httpx
import pytest
from testcontainers.core.container import DockerContainer

KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak:26.8.1"

# ----------------------- #


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


@contextmanager
def keycloak_realm(
    realm: Mapping[str, Any],
    *,
    env: Mapping[str, str] | None = None,
    timeout: float = 180.0,
) -> Iterator[str]:
    """Start Keycloak with *realm* imported and yield the realm's issuer URL.

    :param realm: The realm representation (``realm`` names it).
    :param env: Extra container environment — the values of the realm's ``${ENV}``
        placeholders.
    :param timeout: Seconds to wait for the discovery document.
    """

    with tempfile.TemporaryDirectory() as realm_dir:
        Path(realm_dir, "realm.json").write_text(json.dumps(realm))
        Path(realm_dir).chmod(0o755)
        Path(realm_dir, "realm.json").chmod(0o644)

        container = (
            DockerContainer(KEYCLOAK_IMAGE)
            .with_exposed_ports(8080)
            .with_env("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .with_env("KC_BOOTSTRAP_ADMIN_PASSWORD", secrets.token_hex(16))
            .with_volume_mapping(realm_dir, "/opt/keycloak/data/import", "ro")
            .with_command("start-dev --import-realm")
        )
        for key, value in (env or {}).items():
            container.with_env(key, value)

        with container:
            host = container.get_container_host_ip()
            if not _is_loopback(host):
                pytest.skip(f"the Docker container host is not loopback ({host})")

            issuer = f"http://{host}:{container.get_exposed_port(8080)}/realms/{realm['realm']}"
            _wait_discovery(issuer, timeout=timeout)
            yield issuer


def _wait_discovery(issuer: str, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{issuer}/.well-known/openid-configuration", timeout=2).is_success:
                return
        except httpx.HTTPError as exc:
            last = exc
        time.sleep(0.5)
    raise RuntimeError(f"Keycloak realm {issuer} did not come up within {timeout}s: {last!r}")
