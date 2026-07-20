"""Lifecycle wiring for the SageMaker runtime client."""

from .pool import (
    SageMakerInferenceShutdownHook,
    SageMakerInferenceStartupHook,
    routed_sagemaker_inference_lifecycle_step,
    sagemaker_inference_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "SageMakerInferenceShutdownHook",
    "SageMakerInferenceStartupHook",
    "routed_sagemaker_inference_lifecycle_step",
    "sagemaker_inference_lifecycle_step",
]
