"""Shared inference integration pieces + the dependency-free in-process local adapter."""

from .adapter_common import (
    OUTPUT_MISMATCH_CODE,
    bind_run_options,
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
    "local_inference_lifecycle_step",
    "shape_outputs",
    "validated_instances",
]
