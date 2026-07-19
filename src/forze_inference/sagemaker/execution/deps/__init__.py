"""Deps wiring for SageMaker inference."""

from ...adapters.inference import SAGEMAKER_BACKEND
from .configs import SageMakerInferenceConfig
from .factories import ConfigurableSageMakerInference
from .keys import SageMakerRuntimeClientDepKey
from .module import SageMakerInferenceDepsModule

# ----------------------- #

__all__ = [
    "SAGEMAKER_BACKEND",
    "ConfigurableSageMakerInference",
    "SageMakerInferenceConfig",
    "SageMakerInferenceDepsModule",
    "SageMakerRuntimeClientDepKey",
]
