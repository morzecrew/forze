"""Async HTTP client for model-serving endpoints, with inference error taxonomy."""

from collections.abc import Mapping
from typing import Any, cast, final

from forze_inference.http._compat import require_inference_http

require_inference_http()

# ....................... #

import attrs
import httpx

from forze.base.exceptions import exc
from forze.base.primitives import GuardedLifecycle

from .port import InferenceHttpClientPort

# ----------------------- #

DEFAULT_REQUEST_TIMEOUT_S = 30.0
"""Client-level request timeout when no invocation deadline tightens it."""


def _translate_status(status: int, detail: str) -> Exception:
    """Map an endpoint's HTTP status to the inference error taxonomy."""

    if status == 429:
        return exc.throttled(
            f"Inference endpoint throttled the request: {detail}",
            code="inference_throttled",
        )

    if status == 404:
        return exc.configuration(
            f"Inference endpoint or model not found: {detail}",
            code="inference_route_mismatch",
        )

    if 400 <= status < 500:
        # The server rejected the payload — the wire encoding does not fit the model.
        return exc.validation(
            f"Inference endpoint rejected the request ({status}): {detail}",
            code="inference_output_mismatch",
        )

    return exc.infrastructure(
        f"Inference endpoint failed ({status}): {detail}",
        code="inference_endpoint_unavailable",
    )


# ....................... #


@final
@attrs.define(slots=True)
class InferenceHttpClient(InferenceHttpClientPort):
    """Thin wrapper around :class:`httpx.AsyncClient` for model-serving endpoints."""

    __client: httpx.AsyncClient | None = attrs.field(default=None, init=False)
    __lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)

    # ....................... #

    async def initialize(
        self,
        base_url: str,
        *,
        default_headers: Mapping[str, str] | None = None,
        timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        async def setup() -> None:
            client_kwargs: dict[str, Any] = {
                "base_url": base_url,
                "timeout": httpx.Timeout(timeout_s),
                "headers": dict(default_headers or {}),
            }

            if transport is not None:
                client_kwargs["transport"] = transport

            self.__client = httpx.AsyncClient(**client_kwargs)

        await self.__lifecycle.initialize(
            setup,
            ready=lambda: self.__client is not None,
        )

    # ....................... #

    def _require_client(self) -> httpx.AsyncClient:
        if self.__client is None:
            raise exc.internal("InferenceHttpClient is not initialized")

        return self.__client

    # ....................... #

    async def post_json(
        self,
        path: str,
        body: Mapping[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        client = self._require_client()

        request_timeout = timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT

        try:
            response = await client.post(path, json=dict(body), timeout=request_timeout)

        except httpx.TimeoutException as e:
            raise exc.timeout(
                "Inference endpoint call exceeded its budget.",
                code="inference_timeout",
            ) from e

        except httpx.TransportError as e:
            raise exc.infrastructure(
                f"Inference endpoint unreachable: {e}",
                code="inference_endpoint_unavailable",
            ) from e

        if response.status_code >= 400:
            raise _translate_status(response.status_code, response.text[:500])

        try:
            payload: Any = response.json()

        except ValueError as e:
            raise exc.validation(
                "Inference endpoint returned a non-JSON body.",
                code="inference_output_mismatch",
            ) from e

        if not isinstance(payload, dict):
            raise exc.validation(
                "Inference endpoint returned a non-object JSON body.",
                code="inference_output_mismatch",
            )

        return cast(dict[str, Any], payload)

    # ....................... #

    async def aclose(self) -> None:
        if self.__client is not None:
            await self.__client.aclose()
            self.__client = None

    async def close(self) -> None:
        await self.__lifecycle.close(self.aclose)
