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
    "sagemaker_inference_lifecycle_step",
]
