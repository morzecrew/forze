"""Structural protocol for the SageMaker runtime client."""

from collections.abc import Awaitable
from typing import Any, Protocol

# ----------------------- #


class SageMakerRuntimeClientPort(Protocol):
    """Operations implemented by
    :class:`~forze_inference.sagemaker.kernel.client.SageMakerRuntimeClient`."""

    def invoke_endpoint(
        self,
        endpoint_name: str,
        *,
        body: bytes,
        content_type: str = "application/json",
        accept: str = "application/json",
        target_variant: str | None = None,
        timeout: float | None = None,
    ) -> Awaitable[dict[str, Any]]:
        """Invoke a realtime endpoint and return the decoded JSON response.

        Failures are translated to the inference error taxonomy (throttled / timeout /
        validation / configuration / infrastructure) — callers never see boto exceptions.
        """
        ...  # pragma: no cover

    def close(self) -> Awaitable[None]:
        """Shut down the underlying AWS client."""
        ...  # pragma: no cover
