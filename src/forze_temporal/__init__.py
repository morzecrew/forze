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
from .kernel.client import (
    RoutedTemporalClient,
    TemporalClient,
    TemporalClientPort,
    TemporalConfig,
)
from .kernel.crypto import EncryptingPayloadCodec, encrypting_data_converter
from .sandbox import (
    PASSTHROUGH_MODULES,
    default_sandbox_restrictions,
    sandboxed_workflow_runner,
)
from .saga import TemporalSaga

# ----------------------- #

__all__ = [
    "TemporalConfig",
    "EncryptingPayloadCodec",
    "encrypting_data_converter",
    "TemporalClient",
    "TemporalClientPort",
    "RoutedTemporalClient",
    "TemporalClientDepKey",
    "TemporalDepsModule",
    "TemporalWorkflowConfig",
    "temporal_lifecycle_step",
    "routed_temporal_lifecycle_step",
    "ExecutionContextInterceptor",
    "TemporalSaga",
    "PASSTHROUGH_MODULES",
    "default_sandbox_restrictions",
    "sandboxed_workflow_runner",
]
