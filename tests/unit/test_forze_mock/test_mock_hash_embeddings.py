"""Tests for :class:`forze_mock.MockHashEmbeddingsProvider`."""

import pytest

from forze_mock import MockHashEmbeddingsProvider


@pytest.mark.asyncio
async def test_mock_hash_embeddings_deterministic_and_sized() -> None:
    p = MockHashEmbeddingsProvider(dimensions=4)
    a = await p.embed_one("hello", input_kind="query")
    b = await p.embed_one("hello", input_kind="query")
    c = await p.embed_one("world", input_kind="query")
    assert len(a) == 4
    assert a == b
    assert a != c


@pytest.mark.asyncio
async def test_mock_hash_embeddings_batch() -> None:
    p = MockHashEmbeddingsProvider(dimensions=2)
    rows = await p.embed(["x", "y"])
    assert len(rows) == 2
    assert len(rows[0]) == 2
