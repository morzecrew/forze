"""Structural protocol for the httpx async client."""

from typing import Any, Awaitable, Mapping, Protocol

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
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> Awaitable[Any]:
        """Perform an HTTP request and return the httpx response."""
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
