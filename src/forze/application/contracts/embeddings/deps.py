"""Embeddings dependency keys and routers."""

from ..base import BaseDepPort, DepKey
from .ports import EmbeddingsProviderPort
from .specs import EmbeddingsSpec

# ----------------------- #

EmbeddingsProviderDepPort = BaseDepPort[EmbeddingsSpec, EmbeddingsProviderPort]
"""Build an ``EmbeddingsProviderPort`` for a given ``EmbeddingsSpec``."""

EmbeddingsProviderDepKey = DepKey[EmbeddingsProviderDepPort]("embeddings_provider")
"""Key for registering the ``EmbeddingsProviderPort`` builder implementation."""
