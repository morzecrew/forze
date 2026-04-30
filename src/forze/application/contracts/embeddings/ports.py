"""Port for text embedding providers (e.g. vector search query encoding)."""

from typing import Awaitable, Literal, Protocol, Sequence, runtime_checkable

# ----------------------- #

EmbeddingInputKind = Literal["query", "document"]
"""Hint for providers that use different encodings for queries vs document passages."""

EmbeddingVector = Sequence[float]
EmbeddingVectors = Sequence[EmbeddingVector]

# ....................... #


@runtime_checkable
class EmbeddingsProviderPort(Protocol):
    """Map text to fixed-dimension vectors (search queries, document chunks, etc.)."""

    def embed(
        self,
        texts: Sequence[str],
        *,
        input_kind: EmbeddingInputKind = "document",
    ) -> Awaitable[EmbeddingVectors]:
        """Return one embedding vector per input string, in order.

        :param texts: Input strings to embed. Implementations may reject empty sequences depending on the backend.
        :param input_kind: ``"query"`` for search-time strings, ``"document"`` for indexable text; providers that do not distinguish may ignore this.
        :returns: Embeddings; each row length should match the configured ``EmbeddingsSpec`` dimensions for this port instance.
        """
        ...  # pragma: no cover

    def embed_one(
        self,
        text: str,
        *,
        input_kind: EmbeddingInputKind = "document",
    ) -> Awaitable[EmbeddingVector]:
        """Return a single embedding vector for the input string.

        :param text: Input string to embed.
        :param input_kind: ``"query"`` for search-time strings, ``"document"`` for indexable text; providers that do not distinguish may ignore this.
        :returns: Embedding vector; length should match the configured ``EmbeddingsSpec`` dimensions for this port instance.
        """
        ...  # pragma: no cover
