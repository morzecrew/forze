"""Inference dependency key and router (read-plane)."""

from typing import Any, TypeVar

from pydantic import BaseModel

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import InferencePort
from .specs import InferenceSpec

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseModel)

# ....................... #

InferenceDepPort = ConfigurableDepPort[
    InferenceSpec[Any, Any],
    InferencePort[Any, Any],
]
"""Build an :class:`InferencePort` for a given :class:`InferenceSpec`."""

# ....................... #

InferenceDepKey = DepKey[InferenceDepPort]("inference_query")
"""Key for registering the :class:`InferencePort` builder implementation."""

# ....................... #


class InferenceDeps(ConvenientDeps):
    """Convenience wrapper for inference dependencies.

    Read-plane: invoking a model is a pure read of it, so the port resolves through
    :meth:`~forze.application.contracts.deps.ConvenientDeps._resolve_configurable` and is
    available inside a read-only (``QUERY``) operation — a query handler computing a
    recommendation must be able to call a model. (Offline batch job submission, which
    launches paid external work, will be a separate command-plane port.)
    """

    def model(self, spec: InferenceSpec[In, Out]) -> InferencePort[In, Out]:
        """Resolve the inference port for *spec*; both type parameters propagate."""

        return self._resolve_configurable(
            InferenceDepKey,
            spec,
            route=spec.name,
        )
