"""Pytest configuration for Temporal integration tests (time-skipping test server)."""

import socket

import pytest
import pytest_asyncio
from temporalio.contrib.pydantic import pydantic_data_converter

pytest.importorskip("temporalio")


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
