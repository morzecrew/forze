"""SageMaker inference kernel: the runtime client and its port."""

from .client import SageMakerRuntimeClient
from .port import SageMakerRuntimeClientPort

# ----------------------- #

__all__ = [
    "SageMakerRuntimeClient",
    "SageMakerRuntimeClientPort",
]
