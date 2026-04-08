from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import Callable

import attrs

from forze.base.primitives import uuid4
from forze.infra.tenancy import MultiTenancyMixin

from ..kernel.platform import TemporalClient

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalBaseAdapter(MultiTenancyMixin):
    """Base adapter for Temporal integration."""

    client: TemporalClient
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
