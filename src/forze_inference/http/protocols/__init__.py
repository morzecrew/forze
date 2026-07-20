"""Wire-protocol strategies for served-model endpoints."""

from forze_inference.records import wrap_scalar_predictions

from .base import WireProtocol, WireRequest
from .kserve_v2 import KserveV2Protocol, validate_flat_scalar_fields
from .mlflow import MlflowProtocol

# ----------------------- #

__all__ = [
    "KserveV2Protocol",
    "MlflowProtocol",
    "WireProtocol",
    "WireRequest",
    "validate_flat_scalar_fields",
    "wrap_scalar_predictions",
]
