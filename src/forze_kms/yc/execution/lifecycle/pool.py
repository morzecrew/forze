"""Yandex Cloud KMS client lifecycle hooks and step factory."""

from collections.abc import Mapping
from typing import Any, cast, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import ClientShutdownHook

from ...kernel.client import YcKmsClient, YcKmsConfig
from ..deps import YcKmsClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class YcKmsStartupHook(LifecycleHook):
    """Startup hook that initializes the Yandex Cloud KMS client from the deps container."""

    iam_token: str | None = attrs.field(default=None, repr=False)
    """Short-lived IAM token."""

    oauth_token: str | None = attrs.field(default=None, repr=False)
    """Long-lived OAuth token."""

    service_account_key: Mapping[str, str] | None = attrs.field(default=None, repr=False)
    """Authorized-key JSON for a service account (the SDK refreshes IAM tokens)."""

    config: YcKmsConfig | None = attrs.field(default=None, repr=False)
    """Optional client config (endpoint, request timeout)."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(YcKmsClient, ctx.deps.provide(YcKmsClientDepKey))

        await client.initialize(
            iam_token=self.iam_token,
            oauth_token=self.oauth_token,
            service_account_key=self.service_account_key,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class YcKmsShutdownHook(ClientShutdownHook):
    """Shutdown hook that releases the Yandex Cloud KMS client."""

    dep_key: DepKey[Any] = attrs.field(default=YcKmsClientDepKey, init=False)


# ....................... #


def yckms_lifecycle_step(
    name: str = "yckms_lifecycle",
    *,
    iam_token: str | None = None,
    oauth_token: str | None = None,
    service_account_key: Mapping[str, str] | None = None,
    config: YcKmsConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for Yandex Cloud KMS client init and shutdown.

    :param name: Step name for collision detection.
    :param iam_token: Short-lived IAM token.
    :param oauth_token: Long-lived OAuth token.
    :param service_account_key: Authorized-key JSON for a service account.
    :param config: Optional client config. With no credential the SDK falls back to
        the instance metadata service.
    :returns: Lifecycle step with startup and shutdown hooks.
    """

    return LifecycleStep(
        id=name,
        startup=YcKmsStartupHook(
            iam_token=iam_token,
            oauth_token=oauth_token,
            service_account_key=service_account_key,
            config=config,
        ),
        shutdown=YcKmsShutdownHook(),
    )
