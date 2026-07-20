"""Pytest configuration for forze_firestore integration tests."""

import os
import shutil
import subprocess
from collections.abc import Iterator
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("google.cloud.firestore")
pytest.importorskip("testcontainers")

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from forze_firestore.kernel.client import FirestoreClient

_EMULATOR_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:522.0.0-emulators"
_TEST_PROJECT = "forze-firestore-test"

# The SDK dials FIRESTORE_EMULATOR_HOST directly (insecure gRPC). If
# http_proxy/https_proxy point at 127.0.0.1:1081, gRPC is routed to the proxy
# instead of the emulator — clear proxies for the test session only.
_EMULATOR_PORT = 19280
_PROXY_ENV_KEYS = (
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "all_proxy",
    "socks_proxy",
    "SOCKS_PROXY",
)


@pytest.fixture(scope="session", autouse=True)
def _without_http_proxy_for_firestore_tests() -> Iterator[None]:
    """Disable HTTP proxies for the session so the Firestore SDK reaches the emulator."""
    saved = {key: os.environ.pop(key) for key in _PROXY_ENV_KEYS if key in os.environ}
    yield
    for key, value in saved.items():
        os.environ[key] = value


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Firestore integration tests")


def _stop_conflicting_emulators() -> None:
    subprocess.run(
        ["docker", "rm", "-f", "fs-emu-test"],
        check=False,
        capture_output=True,
    )


@pytest.fixture(scope="session")
def firestore_emulator_container() -> DockerContainer:
    """Start the Firestore emulator on a non-default host port (see module docstring)."""
    _ensure_docker()
    _stop_conflicting_emulators()

    container = (
        DockerContainer(image=_EMULATOR_IMAGE)
        .with_command(
            [
                "gcloud",
                "beta",
                "emulators",
                "firestore",
                "start",
                f"--project={_TEST_PROJECT}",
                f"--host-port=0.0.0.0:{_EMULATOR_PORT}",
            ]
        )
        .with_bind_ports(_EMULATOR_PORT, _EMULATOR_PORT)
    )
    container.start()

    def ready(text: str) -> bool:
        lowered = text.lower()
        return "dev app server is now running" in lowered

    wait_for_logs(container, ready, timeout=120)

    os.environ["FIRESTORE_EMULATOR_HOST"] = f"127.0.0.1:{_EMULATOR_PORT}"

    yield container

    container.stop()
    _stop_conflicting_emulators()


@pytest_asyncio.fixture(scope="function")
async def firestore_client(
    firestore_emulator_container: DockerContainer,
) -> FirestoreClient:
    """Provide an initialized Firestore client connected to the emulator."""
    _ = firestore_emulator_container
    client = FirestoreClient()
    await client.initialize(project_id=_TEST_PROJECT, database="(default)")

    yield client

    await client.close()


@pytest.fixture
def unique_collection() -> str:
    return f"forze_{uuid4().hex[:8]}"
