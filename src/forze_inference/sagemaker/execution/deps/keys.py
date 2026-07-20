"""Dependency keys owned by the SageMaker inference integration."""

from forze.application.contracts.deps import DepKey

from ...kernel import SageMakerRuntimeClientPort

# ----------------------- #

SageMakerRuntimeClientDepKey = DepKey[SageMakerRuntimeClientPort]("sagemaker_runtime_client")
"""Key for the pre-constructed runtime client (initialized via the lifecycle step)."""
