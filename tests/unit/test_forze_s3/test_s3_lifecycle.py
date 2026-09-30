"""Unit tests for :mod:`forze_s3.execution.lifecycle.pool`."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from forze.application.execution import Deps, LifecyclePlan
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze.base.exceptions import CoreException, ExceptionKind
from forze_s3.execution.deps import S3ClientDepKey
from forze_s3.execution.lifecycle import (
    S3_CLIENT_CAPABILITY,
    S3ShutdownHook,
    S3StartupHook,
    s3_bucket_lifecycle_step,
    s3_lifecycle_step,
)
from forze_s3.kernel.client import S3Client, S3Config
from tests.support.execution_context import context_from_deps


@pytest.mark.asyncio
async def test_s3_startup_hook_initializes_client() -> None:
    client = Mock(spec=S3Client)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: client}))
    config = S3Config()
    hook = S3StartupHook(
        endpoint="http://localhost:9000",
        access_key_id="minio",
        secret_access_key="minio123",
        config=config,
    )

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        "http://localhost:9000",
        "minio",
        SecretStr("minio123"),
        config=config,
    )


@pytest.mark.asyncio
async def test_s3_shutdown_hook_closes_client() -> None:
    client = Mock(spec=S3Client)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: client}))
    hook = S3ShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_s3_lifecycle_step_builds_hooks() -> None:
    step = s3_lifecycle_step(
        endpoint="http://localhost:9000",
        access_key_id="minio",
        secret_access_key="minio123",
    )

    assert step.id == "s3_lifecycle"
    assert isinstance(step.startup, S3StartupHook)
    assert isinstance(step.shutdown, S3ShutdownHook)


class _MockRoutedS3:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_client_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedS3()
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_client_lifecycle_step("routed_s3_lifecycle", client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1


# ....................... #


class _BucketClient:
    """An S3 client double: calls outside an initialized ``client()`` scope fail, as the real one."""

    def __init__(self, *, existing: set[str] | None = None, fail_on: str | None = None) -> None:
        self.buckets = set(existing or ())
        self.created: list[str] = []
        self.initialized = False
        self.fail_on = fail_on
        self._scoped = False

    async def initialize(self, *args: object, **kwargs: object) -> None:
        self.initialized = True

    async def close(self) -> None:
        return None

    @asynccontextmanager
    async def client(self) -> AsyncIterator[None]:
        if not self.initialized:
            raise RuntimeError("S3 client is not initialized")

        self._scoped = True

        try:
            yield

        finally:
            self._scoped = False

    async def ensure_bucket(self, bucket: str) -> None:
        if not self._scoped:
            raise RuntimeError("S3 client is not initialized")

        if bucket == self.fail_on:
            raise RuntimeError(f"cannot create {bucket}")

        if bucket not in self.buckets:
            self.buckets.add(bucket)
            self.created.append(bucket)


def _plan(client: _BucketClient, *buckets: str) -> tuple[object, object]:
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: client}))
    # Declared bucket step first: ordering must come from the capability, not the list.
    plan = LifecyclePlan.from_steps(
        s3_bucket_lifecycle_step(buckets=buckets),
        s3_lifecycle_step(endpoint="http://localhost:9000"),
    )
    return plan.freeze(), ctx


class TestTheBucketStep:
    @pytest.mark.asyncio
    async def test_missing_buckets_are_created_after_the_client_starts(self) -> None:
        client = _BucketClient(existing={"assets"})
        plan, ctx = _plan(client, "assets", "exports", "avatars")

        await plan.startup(ctx)  # type: ignore[attr-defined]

        assert client.created == ["exports", "avatars"]
        assert client.buckets == {"assets", "exports", "avatars"}

    @pytest.mark.asyncio
    async def test_a_bucket_that_cannot_be_created_stops_startup(self) -> None:
        client = _BucketClient(fail_on="exports")
        plan, ctx = _plan(client, "assets", "exports")

        with pytest.raises(RuntimeError, match="cannot create exports"):
            await plan.startup(ctx)  # type: ignore[attr-defined]

    def test_the_step_waits_for_the_client_and_marks_shared_state(self) -> None:
        step = s3_bucket_lifecycle_step(buckets=["assets"])

        assert step.id == "s3_buckets"
        assert step.requires == (S3_CLIENT_CAPABILITY,)
        assert step.mutates_shared_state
        assert S3_CLIENT_CAPABILITY in s3_lifecycle_step(endpoint="http://x").provides

    @pytest.mark.parametrize(
        "buckets",
        [[], ["assets", " "], ["assets", ""], "assets", ["assets", None]],
        ids=["none", "blank", "empty", "bare-string", "not-a-string"],
    )
    def test_a_bucket_list_nobody_meant_is_refused(self, buckets: object) -> None:
        with pytest.raises(CoreException) as caught:
            s3_bucket_lifecycle_step(buckets=buckets)  # type: ignore[arg-type]

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    def test_buckets_given_once_through_are_all_kept(self) -> None:
        # A generator is read once: checking it must not leave the step with nothing.
        step = s3_bucket_lifecycle_step(buckets=(name for name in ("assets", "exports")))

        assert step.startup.buckets == ("assets", "exports")  # type: ignore[union-attr]
