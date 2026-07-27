"""Shared inference integration pieces + the dependency-free in-process local adapter."""

from .adapter_common import (
    BUDGET_EXHAUSTED_CODE,
    OUTPUT_MISMATCH_CODE,
    bind_run_options,
    ensure_budget,
    resolve_wire_cap,
    scalar_output_field,
    shape_outputs,
    validated_instances,
)
from .deps_module import (
    ConfigurableLocalInference,
    LocalInferenceDepsModule,
    LocalInferenceWarmupHook,
    local_inference_lifecycle_step,
)
from .local import (
    LOCAL_INFERENCE_BACKEND,
    LocalInferenceAdapter,
    LocalInferenceConfig,
    LocalModel,
    LocalModelHost,
)

# ----------------------- #

__all__ = [
    "BUDGET_EXHAUSTED_CODE",
    "LOCAL_INFERENCE_BACKEND",
    "OUTPUT_MISMATCH_CODE",
    "ConfigurableLocalInference",
    "LocalInferenceAdapter",
    "LocalInferenceConfig",
    "LocalInferenceDepsModule",
    "LocalInferenceWarmupHook",
    "LocalModel",
    "LocalModelHost",
    "bind_run_options",
    "ensure_budget",
    "local_inference_lifecycle_step",
    "resolve_wire_cap",
    "scalar_output_field",
    "shape_outputs",
    "validated_instances",
]
