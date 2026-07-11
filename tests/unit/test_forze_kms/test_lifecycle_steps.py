"""The three KMS lifecycle steps — the client is registered, but never *opened*, by wiring.

A deps module only publishes an already-constructed client; the lifecycle step is what
resolves it and initializes it, so a route that forgets the step fails at the first call
rather than at startup. These pin that each step reaches its client with the credentials
it was given, and that shutdown closes it.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("aioboto3")
pytest.importorskip("google.cloud.kms")
pytest.importorskip("yandexcloud")

from forze.application.contracts.deps import Deps
from forze_kms.aws import AwsKmsClient, AwsKmsClientDepKey, awskms_lifecycle_step
from forze_kms.gcp import GcpKmsClient, GcpKmsClientDepKey, gcpkms_lifecycle_step
from forze_kms.yc import YcKmsClient, YcKmsClientDepKey, yckms_lifecycle_step
from tests.support.execution_context import context_from_deps

# ----------------------- #


def _ctx(dep_key, client):  # type: ignore[no-untyped-def]
    return context_from_deps(Deps.plain({dep_key: client}))


# ....................... #


class TestAwsLifecycle:
    async def test_startup_initializes_the_client_with_its_credentials(self) -> None:
        client = MagicMock(spec=AwsKmsClient)
        client.initialize = AsyncMock()
        step = awskms_lifecycle_step(
            endpoint="http://localhost:1",
            region_name="eu-west-1",
            access_key_id="k",
            secret_access_key="s",
        )

        await step.startup(_ctx(AwsKmsClientDepKey, client))  # type: ignore[misc]

        kwargs = client.initialize.await_args.kwargs
        assert kwargs["endpoint"] == "http://localhost:1"
        assert kwargs["region_name"] == "eu-west-1"
        assert kwargs["access_key_id"] == "k"

    async def test_shutdown_closes_the_client(self) -> None:
        client = MagicMock(spec=AwsKmsClient)
        client.close = AsyncMock()
        step = awskms_lifecycle_step()

        await step.shutdown(_ctx(AwsKmsClientDepKey, client))  # type: ignore[misc]

        client.close.assert_awaited_once()

    def test_the_step_carries_the_given_name(self) -> None:
        assert awskms_lifecycle_step("custom").id == "custom"


# ....................... #


class TestGcpLifecycle:
    async def test_startup_initializes_the_client(self) -> None:
        client = MagicMock(spec=GcpKmsClient)
        client.initialize = AsyncMock()
        step = gcpkms_lifecycle_step(endpoint="localhost:9010")

        await step.startup(_ctx(GcpKmsClientDepKey, client))  # type: ignore[misc]

        assert client.initialize.await_args.kwargs["endpoint"] == "localhost:9010"

    async def test_shutdown_closes_the_client(self) -> None:
        client = MagicMock(spec=GcpKmsClient)
        client.close = AsyncMock()

        await gcpkms_lifecycle_step().shutdown(_ctx(GcpKmsClientDepKey, client))  # type: ignore[misc]

        client.close.assert_awaited_once()

    def test_the_step_carries_the_given_name(self) -> None:
        assert gcpkms_lifecycle_step("custom").id == "custom"


# ....................... #


class TestYcLifecycle:
    async def test_startup_initializes_the_client_with_its_credentials(self) -> None:
        client = MagicMock(spec=YcKmsClient)
        client.initialize = AsyncMock()
        step = yckms_lifecycle_step(iam_token="t", service_account_key={"id": "k"})

        await step.startup(_ctx(YcKmsClientDepKey, client))  # type: ignore[misc]

        kwargs = client.initialize.await_args.kwargs
        assert kwargs["iam_token"] == "t"
        assert kwargs["service_account_key"] == {"id": "k"}

    async def test_shutdown_closes_the_client(self) -> None:
        client = MagicMock(spec=YcKmsClient)
        client.close = AsyncMock()

        await yckms_lifecycle_step().shutdown(_ctx(YcKmsClientDepKey, client))  # type: ignore[misc]

        client.close.assert_awaited_once()

    def test_the_step_carries_the_given_name(self) -> None:
        assert yckms_lifecycle_step("custom").id == "custom"
