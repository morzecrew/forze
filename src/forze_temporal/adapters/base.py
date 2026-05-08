from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import Callable

import attrs

from forze.application.contracts.tenancy import TenancyMixin
from forze.base.primitives import uuid4

from ..kernel.platform import TemporalClientPort

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalBaseAdapter(TenancyMixin):
    """Base adapter for Temporal integration."""

    client: TemporalClientPort
    """Temporal client."""

    workflow_id_factory: Callable[[], str] = attrs.field(default=lambda: str(uuid4))
    """Callable to generate a unique workflow ID."""

    # ....................... #

    def construct_workflow_id(self, workflow_id: str | None = None) -> str:
        """Construct a workflow ID from the attached tenant ID if any."""

        tenant_id = self.require_tenant_if_aware()
        workflow_id = workflow_id or self.workflow_id_factory()

        if tenant_id is not None:
            return f"tenant:{tenant_id}:{workflow_id}"

        return workflow_id
