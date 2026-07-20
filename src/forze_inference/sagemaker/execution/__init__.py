"""Execution wiring for SageMaker inference."""

from .deps import (
    SAGEMAKER_BACKEND,
    ConfigurableSageMakerInference,
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
    SageMakerRuntimeClientDepKey,
)
from .lifecycle import (
    SageMakerInferenceShutdownHook,
    SageMakerInferenceStartupHook,
    routed_sagemaker_inference_lifecycle_step,
    sagemaker_inference_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "SAGEMAKER_BACKEND",
    "ConfigurableSageMakerInference",
    "SageMakerInferenceConfig",
    "SageMakerInferenceDepsModule",
    "SageMakerInferenceShutdownHook",
    "SageMakerInferenceStartupHook",
    "SageMakerRuntimeClientDepKey",
    "routed_sagemaker_inference_lifecycle_step",
    "sagemaker_inference_lifecycle_step",
]
