"""AWS SageMaker realtime inference for the inference seam.

Requires the ``forze[inference-sagemaker]`` extra. One
:class:`SageMakerInferenceDepsModule` binds inference routes to realtime endpoints; the
JSON-record scope sends ``{"instances": [...]}`` and expects ``{"predictions": [...]}``
(the TF-Serving / sklearn container convention).
"""

from ._compat import require_inference_sagemaker

require_inference_sagemaker()

# ....................... #

from .adapters import SageMakerInferenceAdapter
from .execution import (
    SAGEMAKER_BACKEND,
    ConfigurableSageMakerInference,
    SageMakerInferenceConfig,
    SageMakerInferenceDepsModule,
    SageMakerInferenceShutdownHook,
    SageMakerInferenceStartupHook,
    SageMakerRuntimeClientDepKey,
    routed_sagemaker_inference_lifecycle_step,
    sagemaker_inference_lifecycle_step,
)
from .kernel import (
    RoutedSageMakerRuntimeClient,
    SageMakerRoutingCredentials,
    SageMakerRuntimeClient,
    SageMakerRuntimeClientPort,
)

# ----------------------- #

__all__ = [
    "SAGEMAKER_BACKEND",
    "ConfigurableSageMakerInference",
    "RoutedSageMakerRuntimeClient",
    "SageMakerInferenceAdapter",
    "SageMakerInferenceConfig",
    "SageMakerInferenceDepsModule",
    "SageMakerInferenceShutdownHook",
    "SageMakerInferenceStartupHook",
    "SageMakerRoutingCredentials",
    "SageMakerRuntimeClient",
    "SageMakerRuntimeClientDepKey",
    "SageMakerRuntimeClientPort",
    "routed_sagemaker_inference_lifecycle_step",
    "sagemaker_inference_lifecycle_step",
]
