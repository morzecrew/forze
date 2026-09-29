"""The bucket startup step creates missing buckets on a real S3 server, and reruns cleanly."""

from uuid import uuid4

import pytest

pytest.importorskip("aioboto3")

from forze.application.execution import Deps
from forze_s3 import S3ClientDepKey, s3_bucket_lifecycle_step
from forze_s3.kernel.client import S3Client
from tests.support.execution_context import context_from_deps


@pytest.mark.asyncio
async def test_the_step_creates_missing_buckets_and_reruns(
    s3_client: S3Client, s3_bucket: str
) -> None:
    fresh = [f"forze-step-{uuid4().hex[:12]}", f"forze-step-{uuid4().hex[:12]}"]
    step = s3_bucket_lifecycle_step(buckets=[s3_bucket, *fresh])
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: s3_client}))

    await step.startup(ctx)
    await step.startup(ctx)  # a second replica or a restart finds them all

    async with s3_client.client():
        assert [await s3_client.bucket_exists(b) for b in (s3_bucket, *fresh)] == [True] * 3
