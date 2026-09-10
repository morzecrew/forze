"""A reachable daemon and an image already on it — what this adapter needs and will not do.

The adapter refuses to pull, deliberately: a plane that runs unreviewed code does not also
fetch an image nobody named at the moment of a request. So the *test* pulls, once, and what
runs afterwards is exactly what an operator would have put there.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from docker import from_env
from docker.errors import DockerException

from forze.application.contracts.sandbox import SandboxSpec
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionContext
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState
from forze_sandbox_container import ContainerSandbox, ContainerSandboxConfig

# ----------------------- #

IMAGE = "python:3.12-slim"
"""The image every leg here runs in: an interpreter, and nothing that needs the network."""

BLOBS = StorageSpec(name="sandbox_files")
SPEC = SandboxSpec(name="jobs", provenance="untrusted")


@pytest.fixture(scope="session", autouse=True)
def container_image() -> str:
    """Skip the whole module without a daemon; make sure the image is on the one there is."""

    try:
        client = from_env()
        client.ping()

    except DockerException as error:
        pytest.skip(f"Docker is required: {error}")

    try:
        try:
            client.images.get(IMAGE)

        except DockerException:
            client.images.pull(IMAGE)

    finally:
        client.close()

    return IMAGE


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=MockState()))


def container_config(**overrides: Any) -> ContainerSandboxConfig:
    """A route wired the way an application running unreviewed code would wire one."""

    settings: dict[str, Any] = {
        "provenance": "untrusted",
        "image": IMAGE,
        "wall_clock_ceiling": timedelta(seconds=30),
        "max_output_bytes": 64 * 1024,
        "storage": BLOBS,
        "kill_grace": timedelta(milliseconds=300),
    }
    settings.update(overrides)

    return ContainerSandboxConfig(**settings)


def container_sandbox(ctx: ExecutionContext, **overrides: Any) -> ContainerSandbox:
    return ContainerSandbox(spec=SPEC, config=container_config(**overrides), ctx=ctx)
