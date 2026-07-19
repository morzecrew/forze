"""MLflow scoring protocol: ``POST /invocations`` with ``instances`` records."""

from collections.abc import Mapping, Sequence
from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze_inference.records import decode_predictions_body

from .base import WireRequest

# ----------------------- #

MLFLOW_BACKEND = "mlflow"


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MlflowProtocol:
    """The MLflow ``/invocations`` dialect (one model per server; the route's
    ``model_name`` selects the server via the client's base URL, not the path)."""

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> WireRequest:
        _ = model_name  # mlflow serve hosts one model per endpoint

        # Explicit JSON mode: the default codec keeps UUID/datetime/Decimal live in
        # python mode, which is not wire-safe.
        return (
            "/invocations",
            {"instances": [instance.model_dump(mode="json") for instance in instances]},
        )

    # ....................... #

    def decode_response(
        self,
        spec: InferenceSpec[Any, Any],
        body: Mapping[str, Any],
        *,
        expected: int,
    ) -> Sequence[Mapping[str, Any]]:
        _ = expected  # cardinality is enforced by the shared output shaping

        return decode_predictions_body(spec, body, backend=MLFLOW_BACKEND)
