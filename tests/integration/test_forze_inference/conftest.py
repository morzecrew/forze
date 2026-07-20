"""Pytest fixtures for served-model inference integration tests.

Real engines per the fidelity policy: MLServer (an Open Inference Protocol server —
KServe/Seldon/Triton family) for the ``kserve_v2`` wire dialect, and the MLflow scoring
server for the legacy ``/invocations`` dialect. Both serve tiny pure-python models (no ML
framework) mounted/built from ``_assets/`` — the point is proving the wire encoding
against real protocol parsers, not prediction quality.

SageMaker has no admissible free emulator (floci does not implement it; LocalStack gates
it behind the paid Ultimate tier), so the sagemaker adapter keeps stub-based unit
coverage; a live test would be env-gated real-cloud or LocalStack Ultimate per the
managed-cloud fidelity policy.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import httpx
import pytest

pytest.importorskip("testcontainers")

from testcontainers.core.container import DockerContainer

_ASSETS = Path(__file__).parent / "_assets"

_MLSERVER_IMAGE = "seldonio/mlserver:1.6.1"
_MLFLOW_IMAGE = "ghcr.io/mlflow/mlflow:v3.4.0"


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for inference integration tests")


def _wait_http_ok(url: str, *, timeout_s: float = 180.0) -> None:
    deadline = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=2.0).status_code == 200:
                return
        except httpx.HTTPError:
            pass

        time.sleep(1.0)

    raise TimeoutError(f"Server at {url} did not become ready in {timeout_s}s")


# ....................... #


@pytest.fixture(scope="session")
def mlserver_url() -> str:
    """A live MLServer instance serving the `doubler` custom runtime over V2."""

    _ensure_docker()

    container = (
        DockerContainer(_MLSERVER_IMAGE)
        .with_volume_mapping(str(_ASSETS / "mlserver_model"), "/mnt/models", "ro")
        .with_command("mlserver start /mnt/models")
        .with_exposed_ports(8080)
    )
    container.start()

    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(8080)
        url = f"http://{host}:{port}"
        _wait_http_ok(f"{url}/v2/models/doubler/ready")
        yield url
    finally:
        container.stop()


# ....................... #


@pytest.fixture(scope="session")
def mlflow_url() -> str:
    """A live MLflow scoring server serving a pyfunc doubler at ``/invocations``."""

    _ensure_docker()

    container = (
        DockerContainer(_MLFLOW_IMAGE)
        .with_volume_mapping(str(_ASSETS / "mlflow_model"), "/bootstrap", "ro")
        .with_command("python /bootstrap/build_and_serve.py")
        .with_exposed_ports(5000)
    )
    container.start()

    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5000)
        url = f"http://{host}:{port}"
        _wait_http_ok(f"{url}/ping")
        yield url
    finally:
        container.stop()
