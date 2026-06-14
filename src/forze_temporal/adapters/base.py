from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import Callable
from uuid import UUID

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    is_static_named_resource,
    resolve_scoped_namespace,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import OnceCell, uuid4

from ..kernel.client import TemporalClientPort
from ..kernel.relation import resolve_temporal_queue

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalBaseAdapter(TenancyMixin):
    """Base adapter for Temporal integration."""

    client: TemporalClientPort
    """Temporal client."""

    queue: NamedResourceSpec
    """Static or tenant-scoped Temporal task queue name."""

    workflow_id_factory: Callable[[], str] = attrs.field(default=lambda: str(uuid4))
    """Callable to generate a unique workflow ID."""

    _queue_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        if tenant is None:
            if self.tenant_aware:
                raise exc.internal("Tenant ID is required for the Temporal adapter")

            return None

        return tenant.tenant_id

    # ....................... #

    async def _resolved_queue(self) -> str:
        return await resolve_scoped_namespace(
            self.queue,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._queue_cell,
            resolver=resolve_temporal_queue,
        )

    # ....................... #

    async def _prepare_queue(self) -> None:
        if is_static_named_resource(self.queue):
            return

        await self._resolved_queue()

    # ....................... #

    def construct_workflow_id(self, workflow_id: str | None = None) -> str:
        """Construct a workflow ID from the attached tenant ID if any."""

        tenant_id = self.require_tenant_if_aware()
        workflow_id = workflow_id or self.workflow_id_factory()

        if tenant_id is not None:
            return f"tenant:{tenant_id}:{workflow_id}"

        return workflow_id

    # ....................... #

    def construct_schedule_id(self, schedule_id: str | None = None) -> str:
        """Construct a schedule ID from the attached tenant ID if any."""

        tenant_id = self.require_tenant_if_aware()
        schedule_id = schedule_id or self.workflow_id_factory()

        if tenant_id is not None:
            return f"tenant:{tenant_id}:{schedule_id}"

        return schedule_id
