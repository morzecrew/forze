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
from forze_http.execution.deps.keys import HttpClientDepKey
from forze_http.kernel.client import HttpClient, HttpConfig, RoutedHttpClient

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpStartupHook(LifecycleHook):
    """Initialize the httpx async client."""

    base_url: str | None = None
    config: HttpConfig = attrs.field(factory=HttpConfig, repr=False)
    default_headers: dict[str, str] = attrs.field(factory=dict)
    auth_token: SecretStr | None = attrs.field(
        default=None,
        converter=pydantic_secret_converter,
        repr=False,
    )

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        client = cast(HttpClient, ctx.deps.provide(HttpClientDepKey))
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
class HttpShutdownHook(ClientShutdownHook):
    """Close the httpx client."""

    dep_key: DepKey[Any] = attrs.field(default=HttpClientDepKey, init=False)
    close_method: str = attrs.field(default="aclose", init=False)


# ....................... #


def http_lifecycle_step(
    name: str = "http_lifecycle",
    *,
    base_url: str | None = None,
    config: HttpConfig | None = None,
    default_headers: dict[str, str] | None = None,
    auth_token: str | SecretStr | None = None,
) -> LifecycleStep:
    """Build a lifecycle step for :class:`HttpClient` init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=HttpStartupHook(
            base_url=base_url,
            config=config or HttpConfig(),
            default_headers=default_headers or {},
            auth_token=auth_token,  # type: ignore[arg-type]
        ),
        shutdown=HttpShutdownHook(),
    )


# ....................... #


def routed_http_lifecycle_step(
    name: str = "routed_http_lifecycle",
    *,
    client: RoutedHttpClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedHttpClient` registered as :data:`HttpClientDepKey`."""

    return routed_client_lifecycle_step(name, client=client)
