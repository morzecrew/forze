"""Wire-protocol strategy for served-model endpoints (JSON-record scope).

A protocol encodes a batch of validated input instances into one request and decodes the
response into per-instance record mappings; the adapter then shapes those through the
spec's output codec. v1 speaks JSON records only — binary tensor encodings are a later,
separate extension.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec

# ----------------------- #

WireRequest = tuple[str, dict[str, Any]]
"""``(path, JSON body)`` for one batch call."""


class WireProtocol(Protocol):
    """One serving wire dialect: encode a batch request, decode its response."""

    @property
    def instances_per_request(self) -> int | None:
        """How many instances one request of this dialect may carry, or ``None`` for a
        whole batch.

        A dialect whose endpoint scores one instance per call (a chat completion) returns
        ``1``, and the adapter fans a batch out into that many sequential requests. It is
        declared here rather than decided in the adapter because ``native_batch`` is
        derived from it: a dialect that cannot vectorize must not leave the capability
        claiming otherwise, since the in-memory oracle mirrors the declaration in order to
        refuse where the deployment would.

        Must be at least 1 — a dialect carrying no instance per request can serve nothing.
        """
        ...  # pragma: no cover

    def usage_attributes(self, body: Mapping[str, Any]) -> Mapping[str, int]:
        """Span attributes describing what the call consumed, or empty for a dialect that
        reports nothing.

        Returned rather than recorded so the dialect stays a pure encode/decode strategy;
        the adapter puts them on whichever span is current, the way a declared egress is
        tagged.
        """
        ...  # pragma: no cover

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> WireRequest: ...  # pragma: no cover

    def decode_response(
        self,
        spec: InferenceSpec[Any, Any],
        body: Mapping[str, Any],
        *,
        expected: int,
    ) -> Sequence[Mapping[str, Any]]:
        """Return one record mapping per instance, in order."""
        ...  # pragma: no cover
