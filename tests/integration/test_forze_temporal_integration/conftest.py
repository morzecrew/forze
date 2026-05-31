"""Pytest configuration for Temporal integration tests."""

from __future__ import annotations

import socket
from types import SimpleNamespace

import pytest
import pytest_asyncio
from temporalio.contrib.pydantic import pydantic_data_converter

pytest.importorskip("temporalio")
pytest.importorskip("testcontainers")

from forze_temporal.kernel.client import TemporalClient, TemporalConfig

from .temporal_dev_server import (
    TemporalDevTarget,
    ensure_docker_available,
    start_temporal_dev_container,
)


@pytest_asyncio.fixture
async def workflow_env():
    """Temporal test environment with time skipping (no Docker)."""
    from temporalio.testing import WorkflowEnvironment

    env = await WorkflowEnvironment.start_time_skipping()
    try:
        yield env
    finally:
        await env.shutdown()


@pytest_asyncio.fixture
async def workflow_env_with_host_target():
    """Time-skipping env bound to an explicit localhost port (for RoutedTemporalClient secrets)."""

    from temporalio.testing import WorkflowEnvironment

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    env = await WorkflowEnvironment.start_time_skipping(
        port=port, data_converter=pydantic_data_converter
    )
    try:
        yield env, f"127.0.0.1:{port}"

    finally:
        await env.shutdown()


# ....................... #


@pytest.fixture(scope="session")
def temporal_dev_target() -> TemporalDevTarget:
    """Session-scoped Temporal dev server (Docker, Schedules API enabled)."""

    ensure_docker_available()
    container, target = start_temporal_dev_container()
    try:
        yield target
    finally:
        container.stop()


@pytest_asyncio.fixture
async def temporal_dev_env(temporal_dev_target: TemporalDevTarget):
    """Connected Temporal SDK + Forze :class:`TemporalClient` against the dev server."""

    from temporalio.client import Client

    sdk_client = await Client.connect(
        temporal_dev_target.grpc_address,
        data_converter=pydantic_data_converter,
    )
    forze_client = TemporalClient()
    await forze_client.initialize(
        temporal_dev_target.grpc_address,
        config=TemporalConfig(namespace="default"),
    )
    try:
        yield SimpleNamespace(
            client=sdk_client,
            forze_client=forze_client,
            target=temporal_dev_target.grpc_address,
        )
    finally:
        await forze_client.close()
