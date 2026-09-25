"""Docker availability checks for integration tests."""

from __future__ import annotations

import pytest
from docker import from_env
from docker.errors import DockerException

MINIO_IMAGE = "ghcr.io/morzecrew/minio:RELEASE.2025-09-07T16-13-09Z"
"""The MinIO server the S3 and DuckDB suites run against: an unmodified mirror.

Upstream stopped serving its images — ``quay.io/minio/minio`` and Docker Hub's ``minio/minio`` no
longer answer an anonymous pull, so a fresh runner could not start one. This is the same release,
re-hosted with its layers untouched and labelled with its licence and the source it was built
from. It is frozen, with no security updates, which a test double can afford.
"""


def ensure_docker_available() -> None:
    """Skip the current test when Docker is not reachable."""

    client = None
    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required: {exc}")
    finally:
        if client is not None:
            client.close()
