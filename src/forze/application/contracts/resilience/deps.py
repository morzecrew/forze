"""Resilience executor dependency key and resolver."""

from ..deps import ConvenientDeps, DepKey
from .ports import ResilienceExecutorPort

# ----------------------- #

ResilienceExecutorDepKey = DepKey[ResilienceExecutorPort]("resilience_executor")
"""Key for the process-wide resilience executor singleton."""


# ....................... #


class ResilienceDeps(ConvenientDeps):
    """Resolve the registered resilience executor."""

    def __call__(self) -> ResilienceExecutorPort:
        """Resolve the resilience executor (requires a registered module)."""

        return self._require_ctx().deps.provide(ResilienceExecutorDepKey)
