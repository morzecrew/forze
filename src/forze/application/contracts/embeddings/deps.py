"""Embeddings dependency keys and routers."""

from ..base import BaseDepPort, DepKey
from .ports import EmbeddingsProviderPort
from .specs import EmbeddingsSpec

# ----------------------- #

EmbeddingsProviderDepPort = BaseDepPort[EmbeddingsSpec, EmbeddingsProviderPort]
"""Build an :class:`EmbeddingsProviderPort` for a given :class:`EmbeddingsSpec`."""

EmbeddingsProviderDepKey = DepKey[EmbeddingsProviderDepPort]("embeddings_provider")
"""Key for registering the :class:`EmbeddingsProviderPort` builder implementation."""
