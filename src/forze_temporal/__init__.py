"""Temporal.io integration for Forze."""

from ._compat import require_temporal

require_temporal()

# ....................... #

from .execution import (
    TemporalClientDepKey,
    TemporalDepsModule,
    TemporalWorkflowConfig,
    temporal_lifecycle_step,
)
from .interceptors import ExecutionContextInterceptor
from .kernel.platform import TemporalClient, TemporalConfig

# ----------------------- #

__all__ = [
    "TemporalConfig",
    "TemporalClient",
    "TemporalClientDepKey",
    "TemporalDepsModule",
    "TemporalWorkflowConfig",
    "temporal_lifecycle_step",
    "ExecutionContextInterceptor",
]
