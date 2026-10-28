"""Structural protocol for the httpx async client."""

from collections.abc import Awaitable, Mapping
from contextlib import AbstractAsyncContextManager
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

    def stream(
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
    ) -> AbstractAsyncContextManager[Any]:
        """Perform an HTTP request and yield the httpx response with its body unread.

        Read the body incrementally inside the block (``aiter_bytes``, ``aiter_lines``);
        leaving the block closes the response, read or not. A rejected response is read
        whole — held to ``max_response_bytes`` when it is set — before anything is yielded,
        so it raises exactly as :meth:`request` would, or, with *raise_for_status* false, is
        yielded already read. ``max_response_bytes`` does not apply to an accepted body,
        which is never buffered.
        """
        ...  # pragma: no cover

    def send(
        self,
        request: Any,
        *,
        base_url: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> AbstractAsyncContextManager[Any]:
        """Send an ``httpx.Request`` someone else built and yield the response, body unread.

        What an httpx transport needs: the request as an SDK built it, and the response the
        way it arrived. The body streams undecoded and is cut off once it crosses
        ``max_response_bytes`` when that is set — counted on the wire, accepted or not. A
        redirect is returned rather than followed. Transport failures stay httpx exceptions.

        :param request: The request to send. Its URL must be *base_url* or a path below it,
            on the same scheme, host and port, with no userinfo.
        :param base_url: What the request is held to. ``None`` means the client's own base
            URL — on a routed client, the current tenant's.
        :param headers: Replace the request's headers of the same names, after the client's
            own default headers — a tenant's credential — have replaced them.
        :raises CoreException: ``configuration`` with code ``http_request_off_route`` when
            the URL is not under *base_url*, before anything is sent; ``infrastructure``
            when the body crosses ``max_response_bytes``.
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
