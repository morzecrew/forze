"""An httpx transport that sends a vendor SDK's requests through a declared HTTP service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from typing import cast, final

from forze_http._compat import require_http

require_http()

# ....................... #

import attrs
import httpx

from forze.application.contracts.http import HttpServiceSpec
from forze.application.execution.context import remaining_time
from forze.base.exceptions import exc
from forze_http.adapters.http_service import governed_headers, service_headers
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.kernel.client import HttpClientPort

# ----------------------- #


@final
class _LeasedStream(httpx.AsyncByteStream):
    """*inner*, with *lease* released when the caller closes the response."""

    def __init__(self, inner: httpx.AsyncByteStream, lease: AsyncExitStack) -> None:
        self.__inner = inner
        self.__lease = lease

    async def __aiter__(self) -> AsyncIterator[bytes]:
        # httpx closes a response only when its iteration ends normally; a failed one would
        # hold the pooled connection until the caller remembered to close it.
        try:
            async for chunk in self.__inner:
                yield chunk

        finally:
            await self.__lease.aclose()

    async def aclose(self) -> None:
        await self.__lease.aclose()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class HttpServiceTransport(httpx.AsyncBaseTransport):
    """Send every request an ``httpx.AsyncClient`` makes through a declared HTTP service.

    For a vendor SDK that takes an ``httpx.AsyncClient`` and builds its own requests: hand it
    ``httpx.AsyncClient(transport=...)`` with this transport, from
    :func:`~forze_http.http_service_transport`, and each request leaves the way a declared
    operation does. It is held to the service's ``base_url`` (or the current tenant's route),
    its headers of the same names as the service's — the credential among them — are
    replaced by the service's, it forwards the deadline budget and the trace context, and it
    marks the current span when the route is egress-sensitive. It goes out on the service's
    pooled client. A header the SDK sends under another name goes out as the SDK set it.

    The response comes back as it arrived — status, headers, undecoded body — so status
    handling, retries and error types stay the SDK's. Closing the SDK's client does not close
    the pooled client; the lifecycle owns that.
    """

    client: HttpClientPort
    """The service's pooled client, routed per tenant when the service is."""

    config: HttpServiceConfig
    """The service's wiring: its base URL, headers, credential and egress declaration."""

    spec: HttpServiceSpec
    """The declared service, named in refusals."""

    # ....................... #

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        remaining = remaining_time()

        if remaining is not None and remaining <= 0.0:
            raise exc.timeout(
                f"Invocation deadline exceeded before a request to HTTP service "
                f"{str(self.spec.name)!r}",
                code="deadline_exceeded",
            )

        headers = governed_headers(self.config, service_headers(self.config))
        # A tenant-routed service has no base URL of its own: the routed client holds the
        # request to the current tenant's.
        base_url = None if self.config.tenant_aware else self.config.base_url
        lease = AsyncExitStack()

        try:
            response: httpx.Response = await lease.enter_async_context(
                self.client.send(request, base_url=base_url, headers=headers)
            )

        except BaseException:
            await lease.aclose()
            raise

        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            # The client's response always streams asynchronously; httpx types it as either.
            stream=_LeasedStream(cast(httpx.AsyncByteStream, response.stream), lease),
            extensions=response.extensions,
        )
