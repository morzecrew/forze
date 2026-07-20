"""Structural protocol for the inference HTTP client."""

from collections.abc import Awaitable, Mapping
from typing import Any, Protocol

# ----------------------- #


class InferenceHttpClientPort(Protocol):
    """Operations implemented by :class:`~forze_inference.http.kernel.client.InferenceHttpClient`."""

    def post_json(
        self,
        path: str,
        body: Mapping[str, Any],
        *,
        timeout: float | None = None,
    ) -> Awaitable[dict[str, Any]]:
        """POST a JSON body and return the decoded JSON response.

        Failures are translated to the inference error taxonomy (throttled / timeout /
        validation / configuration / infrastructure) — callers never see transport
        exceptions.
        """
        ...  # pragma: no cover

    def close(self) -> Awaitable[None]:
        """Shut down the underlying HTTP client."""
        ...  # pragma: no cover
