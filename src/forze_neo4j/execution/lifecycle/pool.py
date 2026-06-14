"""Neo4j client lifecycle hooks and step factory."""

from typing import Any, cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import Neo4jClient, Neo4jConfig, RoutedNeo4jClient
from ..deps import Neo4jClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Neo4jStartupHook(LifecycleHook):
    """Startup hook that opens the Neo4j driver from the deps container."""

    uri: SecretStr = attrs.field(converter=pydantic_secret_converter, repr=False)
    """Bolt connection URI (e.g. ``neo4j://host:7687``)."""

    auth: tuple[str, str] | None = attrs.field(default=None, repr=False)
    """Optional ``(user, password)`` basic auth."""

    config: Neo4jConfig = attrs.field(factory=Neo4jConfig, repr=False)
    """Driver pool/timeout configuration."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(Neo4jClient, ctx.deps.provide(Neo4jClientDepKey))
        await client.initialize(self.uri, auth=self.auth, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Neo4jShutdownHook(ClientShutdownHook):
    """Shutdown hook that closes the Neo4j client."""

    dep_key: DepKey[Any] = attrs.field(default=Neo4jClientDepKey, init=False)


# ....................... #


def neo4j_lifecycle_step(
    name: str = "neo4j_lifecycle",
    *,
    uri: str | SecretStr,
    auth: tuple[str, str] | None = None,
    config: Neo4jConfig = Neo4jConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Neo4j driver init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=Neo4jStartupHook(uri=uri, auth=auth, config=config),
        shutdown=Neo4jShutdownHook(),
    )


# ....................... #


def routed_neo4j_lifecycle_step(
    name: str = "routed_neo4j_lifecycle",
    *,
    client: RoutedNeo4jClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedNeo4jClient` registered as :data:`Neo4jClientDepKey`.

    Use with a routed client (secrets-backed per-tenant credentials); do not also use
    :func:`neo4j_lifecycle_step` with a routed client.
    """

    return routed_client_lifecycle_step(name, client=client)
