"""Structural protocol for the httpx async client."""

from collections.abc import Awaitable, Mapping
from typing import Any, Protocol

from forze.base.primitives import JsonDict

# ----------------------- #


class HttpClientPort(Protocol):
    """Operations implemented by :class:`~forze_http.kernel.client.client.HttpClient`."""

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: JsonDict | None = None,
        data: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        raise_for_status: bool = True,
    ) -> Awaitable[Any]:
        """Perform an HTTP request and return the httpx response.

        At most one body: *json* for a JSON request, *data* for
        ``application/x-www-form-urlencoded``.

        With *raise_for_status* false, a rejected response is **returned** instead of
        raised, so a caller that has something to say about the body can read it before
        deciding. The caller then owns the refusal — and owes one.
        """
        ...  # pragma: no cover

    def aclose(self) -> Awaitable[None]:
        """Close the underlying client."""
        ...  # pragma: no cover

    def startup(self) -> Awaitable[None]:
        """Initialize pooled resources (routed clients)."""
        ...  # pragma: no cover

    def close(self) -> Awaitable[None]:
        """Shut down pooled resources (routed clients)."""
        ...  # pragma: no cover

    def evict_tenant(self, tenant_id: Any) -> Awaitable[None]:
        """Evict a tenant client from the pool (routed clients)."""
        ...  # pragma: no cover
