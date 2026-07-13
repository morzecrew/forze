from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from collections.abc import Callable

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

    workflow_id_factory: Callable[[], str] = attrs.field(default=lambda: str(uuid4()))
    """Callable to generate a unique workflow ID."""

    _queue_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

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

    def _tenant_id_prefix(self) -> str | None:
        """Id prefix for the active tenant, or ``None`` when not tenant-aware."""

        tenant_id = self.require_tenant_if_aware()

        return f"tenant:{tenant_id}:" if tenant_id is not None else None

    # ....................... #

    def construct_workflow_id(self, workflow_id: str | None = None) -> str:
        """Construct a workflow ID from the attached tenant ID if any."""

        prefix = self._tenant_id_prefix()
        workflow_id = workflow_id or self.workflow_id_factory()

        if prefix is not None:
            return f"{prefix}{workflow_id}"

        return workflow_id

    # ....................... #

    def construct_schedule_id(self, schedule_id: str | None = None) -> str:
        """Construct a schedule ID from the attached tenant ID if any."""

        prefix = self._tenant_id_prefix()
        schedule_id = schedule_id or self.workflow_id_factory()

        if prefix is not None:
            return f"{prefix}{schedule_id}"

        return schedule_id

    # ....................... #

    def _resolve_scoped_id(self, raw: str, *, label: str) -> str:
        """Resolve a caller-supplied id into the active tenant's id-space.

        Accepts both the raw id a caller used at create/start time and the
        already-prefixed id returned in handles, so both round-trip to the same
        server-side id. An id carrying another tenant's marker is a cross-tenant
        reference and is refused before it reaches the server. Without tenancy
        the id passes through verbatim.
        """

        prefix = self._tenant_id_prefix()

        if prefix is None:
            return raw

        if raw.startswith(prefix):
            return raw

        if raw.startswith("tenant:"):
            raise exc.precondition(
                f"Temporal {label} id {raw!r} is outside the active tenant's namespace",
                code="core.temporal.id_outside_tenant",
            )

        return f"{prefix}{raw}"

    # ....................... #

    def resolve_workflow_id(self, workflow_id: str) -> str:
        """Resolve a workflow id into the active tenant's id-space."""

        return self._resolve_scoped_id(workflow_id, label="workflow")

    # ....................... #

    def resolve_schedule_id(self, schedule_id: str) -> str:
        """Resolve a schedule id into the active tenant's id-space."""

        return self._resolve_scoped_id(schedule_id, label="schedule")
