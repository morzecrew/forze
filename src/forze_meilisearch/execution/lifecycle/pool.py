"""Lifecycle hooks for Meilisearch client initialization and shutdown."""

from typing import Any, cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)
from forze.base.serialization.pydantic import pydantic_secret_converter
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey
from forze_meilisearch.kernel.client import (
    MeilisearchClient,
    MeilisearchConfig,
    RoutedMeilisearchClient,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MeilisearchStartupHook(LifecycleHook):
    """Initialize the Meilisearch async client from configuration."""

    url: str
    api_key: SecretStr | None = attrs.field(
        default=None,
        converter=pydantic_secret_converter,
        repr=False,
    )
    config: MeilisearchConfig = attrs.field(factory=MeilisearchConfig, repr=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(MeilisearchClient, ctx.deps.provide(MeilisearchClientDepKey))
        await client.initialize(self.url, self.api_key, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MeilisearchShutdownHook(ClientShutdownHook):
    """Close the Meilisearch client."""

    dep_key: DepKey[Any] = attrs.field(default=MeilisearchClientDepKey, init=False)
    close_method: str = attrs.field(default="aclose", init=False)


# ....................... #


def meilisearch_lifecycle_step(
    name: str = "meilisearch_lifecycle",
    *,
    url: str,
    api_key: str | SecretStr | None = None,
    config: MeilisearchConfig | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for Meilisearch client init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=MeilisearchStartupHook(
            url=url,
            api_key=api_key,  # type: ignore[arg-type]
            config=config or MeilisearchConfig(),
        ),
        shutdown=MeilisearchShutdownHook(),
    )


# ....................... #


def routed_meilisearch_lifecycle_step(
    name: str = "routed_meilisearch_lifecycle",
    *,
    client: RoutedMeilisearchClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedMeilisearchClient` registered as :data:`MeilisearchClientDepKey`."""

    return routed_client_lifecycle_step(name, client=client)
