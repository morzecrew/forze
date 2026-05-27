"""Embeddings dependency keys and routers."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import EmbeddingsProviderPort
from .specs import EmbeddingsSpec

# ----------------------- #

EmbeddingsProviderDepPort = ConfigurableDepPort[EmbeddingsSpec, EmbeddingsProviderPort]
"""Build an ``EmbeddingsProviderPort`` for a given ``EmbeddingsSpec``."""

EmbeddingsProviderDepKey = DepKey[EmbeddingsProviderDepPort]("embeddings_provider")
"""Key for registering the ``EmbeddingsProviderPort`` builder implementation."""

# ....................... #


class EmbeddingsDeps(ConvenientDeps):
    """Convenience wrapper for embeddings dependencies."""

    def provider(self, spec: EmbeddingsSpec) -> EmbeddingsProviderPort:
        """Resolve an embeddings provider for the given spec."""

        return self._resolve_configurable(
            EmbeddingsProviderDepKey,
            spec,
            route=spec.name,
        )
