"""Lifecycle wiring for the SageMaker runtime client."""

from .pool import (
    SageMakerInferenceShutdownHook,
    SageMakerInferenceStartupHook,
    sagemaker_inference_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "SageMakerInferenceShutdownHook",
    "SageMakerInferenceStartupHook",
    "sagemaker_inference_lifecycle_step",
]
