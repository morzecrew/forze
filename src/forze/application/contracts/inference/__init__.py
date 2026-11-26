"""Inference contracts: typed model invocation behind one hexagonal seam.

One :class:`InferenceSpec` names one logical inference task (typed input/output models);
the wiring config binds it to a physical model — an in-process artifact, a served
endpoint, or a cloud endpoint — so swapping backends is a wiring change with zero
handler edits.
"""

from .capabilities import (
    DEFAULT_INFERENCE_CAPABILITIES,
    FULL_INFERENCE_CAPABILITIES,
    UNSUPPORTED_INFERENCE_FEATURE_CODE,
    InferenceCapabilities,
    validate_batch_size,
    validate_stream_supported,
)
from .deps import (
    InferenceDepKey,
    InferenceDepPort,
    InferenceDeps,
)
from .metering import (
    InferenceMeter,
    InferenceUsage,
    record_inference_usage,
)
from .ports import (
    BaseInferencePort,
    InferencePort,
)
from .specs import (
    InferenceSpec,
    validate_inference_spec,
)
from .types import InferenceRunOptions

# ----------------------- #

__all__ = [
    "DEFAULT_INFERENCE_CAPABILITIES",
    "FULL_INFERENCE_CAPABILITIES",
    "UNSUPPORTED_INFERENCE_FEATURE_CODE",
    "BaseInferencePort",
    "InferenceCapabilities",
    "InferenceDepKey",
    "InferenceDepPort",
    "InferenceDeps",
    "InferenceMeter",
    "InferencePort",
    "InferenceRunOptions",
    "InferenceSpec",
    "InferenceUsage",
    "record_inference_usage",
    "validate_batch_size",
    "validate_inference_spec",
    "validate_stream_supported",
]
