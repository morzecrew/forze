"""Temporal.io integration for Forze."""

from ._compat import require_temporal

require_temporal()

# ....................... #

from .execution import (
    TemporalClientDepKey,
    TemporalDepsModule,
    TemporalWorkflowConfig,
    routed_temporal_lifecycle_step,
    temporal_lifecycle_step,
)
from .interceptors import ExecutionContextInterceptor
from .kernel.platform import (
    RoutedTemporalClient,
    TemporalClient,
    TemporalClientPort,
    TemporalConfig,
)

# ----------------------- #

__all__ = [
    "TemporalConfig",
    "TemporalClient",
    "TemporalClientPort",
    "RoutedTemporalClient",
    "TemporalClientDepKey",
    "TemporalDepsModule",
    "TemporalWorkflowConfig",
    "temporal_lifecycle_step",
    "routed_temporal_lifecycle_step",
    "ExecutionContextInterceptor",
]
