"""Route configs for SageMaker realtime inference."""

from typing import final

import attrs

from forze.application.contracts.resolution import NamedResourceSpec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SageMakerInferenceConfig(TenantAwareIntegrationConfig):
    """Wiring config for one SageMaker realtime inference route.

    Features leave the process (and the VPC boundary, unless the endpoint is private) in
    plaintext — the model needs real values, so field encryption cannot apply. The
    operator must state that consciously via :attr:`acknowledge_data_egress`; wiring
    fails closed until then.
    """

    endpoint_name: NamedResourceSpec
    """SageMaker endpoint name — a static name or a ``(tenant_id) -> name`` resolver for
    per-tenant endpoints (namespace-tier isolation)."""

    target_variant: str | None = None
    """Optional production-variant pin (canary targeting is a wiring fact, not a
    per-call choice)."""

    acknowledge_data_egress: bool = False
    """Must be ``True``: an explicit statement that this route sends feature values in
    plaintext to an external endpoint."""

    max_batch_size: int | None = None
    """Hard per-call instance cap the endpoint imposes, or ``None`` for unbounded.
    ``predict_many`` refuses an oversized batch whole; ``predict_stream`` sub-batches
    its wire calls to the cap."""

    deterministic: bool = False
    """Declare that the deployed model returns the same output for the same input
    (advertised via capabilities; the adapter cannot verify it)."""

    content_type: str = "application/json"
    """Request content type; the JSON-record scope sends ``{"instances": [...]}``."""

    accept: str = "application/json"
    """Response accept type; the JSON-record scope expects ``{"predictions": [...]}``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.acknowledge_data_egress:
            raise exc.configuration(
                "SageMakerInferenceConfig requires acknowledge_data_egress=True: this "
                "route sends feature values in plaintext to an external endpoint, and "
                "the operator must state that consciously."
            )
