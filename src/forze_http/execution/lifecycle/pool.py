"""Lifecycle hooks for httpx client initialization and shutdown."""

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
from forze_http.execution.deps.keys import HttpxClientDepKey
from forze_http.kernel.client import HttpxClient, HttpxConfig, RoutedHttpxClient

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpxStartupHook(LifecycleHook):
    """Initialize the httpx async client."""

    base_url: str | None = None
    config: HttpxConfig = attrs.field(factory=HttpxConfig, repr=False)
    default_headers: dict[str, str] = attrs.field(factory=dict)
    auth_token: SecretStr | None = attrs.field(
        default=None,
        converter=pydantic_secret_converter,
        repr=False,
    )

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(HttpxClient, ctx.deps.provide(HttpxClientDepKey))
        headers = dict(self.default_headers)

        if self.auth_token is not None:
            token = self.auth_token.get_secret_value()

            if token:
                headers.setdefault("Authorization", f"Bearer {token}")

        await client.initialize(
            self.base_url,
            config=self.config,
            default_headers=headers,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpxShutdownHook(ClientShutdownHook):
    """Close the httpx client."""

    dep_key: DepKey[Any] = attrs.field(default=HttpxClientDepKey, init=False)
    close_method: str = attrs.field(default="aclose", init=False)


# ....................... #


def http_lifecycle_step(
    name: str = "http_lifecycle",
    *,
    base_url: str | None = None,
    config: HttpxConfig | None = None,
    default_headers: dict[str, str] | None = None,
    auth_token: str | SecretStr | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for :class:`HttpxClient` init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=HttpxStartupHook(
            base_url=base_url,
            config=config or HttpxConfig(),
            default_headers=default_headers or {},
            auth_token=auth_token,  # type: ignore[arg-type]
        ),
        shutdown=HttpxShutdownHook(),
    )


# ....................... #


def routed_http_lifecycle_step(
    name: str = "routed_http_lifecycle",
    *,
    client: RoutedHttpxClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedHttpxClient` registered as :data:`HttpxClientDepKey`."""

    return routed_client_lifecycle_step(name, client=client)
