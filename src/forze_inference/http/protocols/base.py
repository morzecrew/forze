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
