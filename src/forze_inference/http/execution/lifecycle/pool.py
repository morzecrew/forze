"""Endpoint-client lifecycle hooks and step factory."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.lifecycle.builtin import (
    ClientShutdownHook,
    routed_client_lifecycle_step,
)
from forze.base.primitives import StrKey

from ...kernel import (
    DEFAULT_REQUEST_TIMEOUT_S,
    InferenceHttpClient,
    RoutedInferenceHttpClient,
)
from ..deps.keys import InferenceHttpClientDepKey

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class InferenceHttpStartupHook(LifecycleHook):
    """Initialize the endpoint client registered under ``InferenceHttpClientDepKey``."""

    base_url: str
    """Model-serving endpoint base URL."""

    default_headers: Mapping[str, str] | None = attrs.field(default=None, repr=False)
    """Headers sent on every request (auth tokens resolved by the composition root)."""

    timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S
    """Client-level request timeout when no invocation deadline tightens it."""

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext") -> None:
        client = ctx.deps.provide(InferenceHttpClientDepKey)

        if not isinstance(client, InferenceHttpClient):
            return  # a custom port implementation owns its own initialization

        await client.initialize(
            self.base_url,
            default_headers=self.default_headers,
            timeout_s=self.timeout_s,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class InferenceHttpShutdownHook(ClientShutdownHook):
    """Close the endpoint client on shutdown."""

    dep_key: DepKey[Any] = attrs.field(default=InferenceHttpClientDepKey, init=False)


# ....................... #


def inference_http_lifecycle_step(
    base_url: str,
    *,
    default_headers: Mapping[str, str] | None = None,
    timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
    name: StrKey = "inference_http_client",
    depends_on: tuple[StrKey, ...] = (),
) -> LifecycleStep:
    """Lifecycle step initializing and closing the served-model endpoint client."""

    return LifecycleStep(
        id=name,
        depends_on=depends_on,
        startup=InferenceHttpStartupHook(
            base_url=base_url,
            default_headers=default_headers,
            timeout_s=timeout_s,
        ),
        shutdown=InferenceHttpShutdownHook(),
    )


# ....................... #


def routed_inference_http_lifecycle_step(
    client: RoutedInferenceHttpClient,
    *,
    name: StrKey = "routed_inference_http_client",
) -> LifecycleStep:
    """Lifecycle step for a tenant-routed served-model client (``dedicated`` isolation).

    Unlike the single-client step there is no base URL here — each tenant's endpoint comes
    from its own secret, resolved on first use.
    """

    return routed_client_lifecycle_step(str(name), client=client)
