"""Route configs for served-model inference over HTTP."""

from typing import Any, Literal, final

import attrs

from forze.application.contracts.inference import InferenceSpec
from forze.application.contracts.resolution import NamedResourceSpec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

from ...protocols import (
    KserveV2Protocol,
    MlflowProtocol,
    WireProtocol,
    validate_flat_scalar_fields,
)

# ----------------------- #

InferenceWireProtocolName = Literal["kserve_v2", "mlflow"]
"""Supported serving dialects (JSON-record scope)."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpInferenceConfig(TenantAwareIntegrationConfig):
    """Wiring config for one served-model inference route.

    Features cross the process (and usually the network) boundary **in plaintext** — the
    model needs real values, so field encryption cannot apply. The operator must state
    that consciously via :attr:`acknowledge_data_egress`; wiring fails closed until then.
    """

    protocol: InferenceWireProtocolName
    """Which wire dialect the endpoint speaks. ``kserve_v2`` covers KServe, mlserver,
    Seldon and Triton's HTTP frontend; ``mlflow`` is the legacy ``/invocations`` scoring
    protocol."""

    model_name: NamedResourceSpec
    """Server-side model id — a static name or a ``(tenant_id) -> name`` resolver for
    per-tenant models (namespace-tier isolation)."""

    acknowledge_data_egress: bool = False
    """Must be ``True``: an explicit statement that this route sends feature values in
    plaintext to an external endpoint."""

    max_batch_size: int | None = None
    """Hard per-call instance cap the endpoint imposes, or ``None`` for unbounded.
    ``predict_many`` refuses an oversized batch whole; ``predict_stream`` sub-batches
    its wire calls to the cap."""

    deterministic: bool = False
    """Declare that the served model returns the same output for the same input
    (advertised via capabilities; the adapter cannot verify it)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.acknowledge_data_egress:
            raise exc.configuration(
                "HttpInferenceConfig requires acknowledge_data_egress=True: this route "
                "sends feature values in plaintext to an external endpoint, and the "
                "operator must state that consciously."
            )

    # ....................... #

    def validate_against_spec(self, spec: InferenceSpec[Any, Any]) -> None:
        """Fail-closed spec↔config check, run by the factory at resolve time."""

        if self.protocol == "kserve_v2":
            validate_flat_scalar_fields(spec)

    # ....................... #

    def wire_protocol(self) -> WireProtocol:
        if self.protocol == "kserve_v2":
            return KserveV2Protocol()

        return MlflowProtocol()
