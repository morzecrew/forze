"""Pytest fixtures for served-model inference integration tests.

Real engines per the fidelity policy: MLServer (an Open Inference Protocol server —
KServe/Seldon/Triton family) for the ``kserve_v2`` wire dialect, and the MLflow scoring
server for the legacy ``/invocations`` dialect. Both serve tiny pure-python models (no ML
framework) mounted/built from ``_assets/`` — the point is proving the wire encoding
against real protocol parsers, not prediction quality.

SageMaker runs against **moto** (``motoserver/moto``) — a free, OSS, independent
reimplementation of the AWS wire protocol, and therefore admissible under the fidelity
policy (unlike an engine-proxying emulator). floci does not implement SageMaker at all,
and LocalStack gates it behind the paid Ultimate tier. moto answers ``InvokeEndpoint``
from a canned-result queue configured via ``/moto-api/static/sagemaker/endpoint-results``:
each *distinct* request body consumes the next queued result and is then memoized, so
repeating a body replays its result. What this proves is the real aioboto3 client, real
SigV4 signing, real botocore response parsing, and our decode/error-translation path — it
does not exercise a model container (moto has none), so prediction content is canned by
construction.
"""

from __future__ import annotations

import json
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
_MOTO_IMAGE = "motoserver/moto:5.1.22"

MOTO_ACCOUNT_ID = "123456789012"
MOTO_REGION = "eu-west-1"


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


# ....................... #


@pytest.fixture(scope="session")
def moto_url() -> str:
    """A live moto server answering the SageMaker runtime wire protocol."""

    _ensure_docker()

    container = DockerContainer(_MOTO_IMAGE).with_exposed_ports(5000)
    container.start()

    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5000)
        url = f"http://{host}:{port}"
        _wait_http_ok(f"{url}/moto-api/data.json")
        yield url
    finally:
        container.stop()


@pytest.fixture
def sagemaker_results(moto_url: str):
    """Reset moto, then queue canned ``InvokeEndpoint`` responses for one test.

    Returns a callable taking response bodies (each a JSON-serializable payload). Each
    *distinct* request body the adapter sends consumes the next queued response in order;
    an identical body replays its earlier response.
    """

    httpx.post(f"{moto_url}/moto-api/reset", timeout=10.0).raise_for_status()

    def queue(*bodies: object) -> None:
        httpx.post(
            f"{moto_url}/moto-api/static/sagemaker/endpoint-results",
            json={
                "account_id": MOTO_ACCOUNT_ID,
                "region": MOTO_REGION,
                "results": [
                    {
                        "Body": json.dumps(body),
                        "ContentType": "application/json",
                        "InvokedProductionVariant": "blue",
                        "CustomAttributes": "",
                    }
                    for body in bodies
                ],
            },
            timeout=10.0,
        ).raise_for_status()

    return queue
