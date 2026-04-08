"""Pytest configuration for Temporal integration tests (time-skipping test server)."""

import pytest
import pytest_asyncio

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
