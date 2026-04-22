"""Tests for forze.application.contracts.embeddings."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from forze.application.contracts.embeddings import (
    EmbeddingsProviderPort,
    EmbeddingsSpec,
    EmbeddingInputKind,
)
from forze.base.errors import CoreError


class _StubEmbeddings:
    async def embed(
        self,
        texts: Sequence[str],
        *,
        input_kind: EmbeddingInputKind = "document",
    ) -> list[tuple[float, ...]]:
        _ = input_kind
        return [tuple(1.0 for _ in t) for t in texts]

    async def embed_one(
        self,
        text: str,
        *,
        input_kind: EmbeddingInputKind = "document",
    ) -> tuple[float, ...]:
        _ = input_kind
        return tuple(1.0 for _ in text)


def test_embeddings_spec_rejects_non_positive_dimensions() -> None:
    with pytest.raises(CoreError, match="positive"):
        EmbeddingsSpec(name="e", dimensions=0)


def test_embeddings_provider_port_structural() -> None:
    stub: EmbeddingsProviderPort = _StubEmbeddings()
    assert stub is not None


@pytest.mark.asyncio
async def test_stub_embed() -> None:
    stub = _StubEmbeddings()
    out = await stub.embed(["a", "ab"], input_kind="query")
    assert [len(r) for r in out] == [1, 2]
