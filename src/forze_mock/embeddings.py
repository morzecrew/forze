"""Deterministic mock :class:`~forze.application.contracts.embeddings.EmbeddingsProviderPort`."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

import attrs

from forze.application.contracts.embeddings import EmbeddingInputKind

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class MockHashEmbeddingsProvider:
    """Return a fixed-size vector from SHA-256 mixing of the input + dimension index.

    Each component is ``int.from_bytes(digest[:8]) / 1e18`` for a stable, test-friendly
    embedding that does not call external APIs.
    """

    dimensions: int
    """Output vector length."""

    # ....................... #

    def _one(self, text: str) -> tuple[float, ...]:
        out: list[float] = []
        for i in range(self.dimensions):
            digest = hashlib.sha256(f"{text}\0{i}".encode()).digest()
            n = int.from_bytes(digest[:8], "big", signed=False)
            out.append(n / 1e18)
        return tuple(out)

    # ....................... #

    async def embed(
        self,
        texts: Sequence[str],
        *,
        input_kind: EmbeddingInputKind = "document",
    ) -> list[tuple[float, ...]]:
        _ = input_kind
        return [self._one(t) for t in texts]

    async def embed_one(
        self,
        text: str,
        *,
        input_kind: EmbeddingInputKind = "document",
    ) -> tuple[float, ...]:
        _ = input_kind
        return self._one(text)
