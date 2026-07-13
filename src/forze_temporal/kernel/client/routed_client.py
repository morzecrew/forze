"""Temporal client that resolves a server address per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from collections.abc import Callable, Mapping, Sequence
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel
from temporalio.client import WorkflowHandle

from forze.application.contracts.durable.workflow import (
    DurableWorkflowRunDescription,
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleTiming,
)
from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import DsnRoutedTenantClientBase
from forze.base.exceptions import exc

from .client import TemporalClient
from .port import TemporalClientPort
from .schedule_types import TemporalScheduleListPage
from .value_objects import TemporalConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedTemporalClient(DsnRoutedTenantClientBase[TemporalClient], TemporalClientPort):
    """Routes each call to a lazily created :class:`TemporalClient` for the current tenant.

    Host strings (for example ``localhost:7233``) are resolved via
    :meth:`SecretsPort.resolve_str` and ``secret_ref_for_tenant``. Shared
    namespace, lazy mode, and interceptors come from ``connection_config``.

    Register this instance under :data:`~forze_temporal.execution.deps.TemporalClientDepKey` and
    use :func:`~forze_temporal.execution.lifecycle.routed_temporal_lifecycle_step` for startup/shutdown.

    Do not combine with :func:`~forze_temporal.execution.lifecycle.temporal_lifecycle_step` on the same
    registered instance.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    connection_config: TemporalConfig = attrs.field(factory=TemporalConfig)
    max_cached_tenants: int = 100
    dsn_backend: str = attrs.field(default="Temporal", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed Temporal access",
        init=False,
    )

    # ....................... #

    async def initialize_client(self, tenant_id: UUID, creds: str) -> TemporalClient:
        client = TemporalClient()
        await client.initialize(creds, config=self.connection_config)

        return client

    # ....................... #

    def access_fingerprint_extra_parts(self, tenant_id: UUID) -> Sequence[str]:
        return [self.connection_config.namespace]

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()

        return await inner.health()

    # ....................... #

    async def start_workflow(
        self,
        queue: str,
        name: str,
        arg: BaseModel,
        *,
        workflow_id: str,
        raise_on_already_started: bool = True,
    ) -> WorkflowHandle[Any, Any]:
        inner = await self._get_client()
        return await inner.start_workflow(
            queue,
            name,
            arg,
            workflow_id=workflow_id,
            raise_on_already_started=raise_on_already_started,
        )

    # ....................... #

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> WorkflowHandle[Any, Any]:
        self._pool.require_started()

        tid = self._require_tenant_id()
        inner = self._peek_client(tid)

        if inner is None:
            raise exc.internal(
                "No Temporal client for this tenant in cache; call an async method "
                "(e.g. :meth:`start_workflow` or :meth:`health`) first to connect.",
            )

        return inner.get_workflow_handle(
            workflow_id,
            run_id=run_id,
            result_type=result_type,
        )

    # ....................... #

    async def signal_workflow(
        self,
        workflow_id: str,
        *,
        signal: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> None:
        inner = await self._get_client()
        await inner.signal_workflow(
            workflow_id,
            signal=signal,
            arg=arg,
            run_id=run_id,
        )

    # ....................... #

    async def query_workflow(
        self,
        workflow_id: str,
        *,
        query: str,
        arg: BaseModel,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.query_workflow(
            workflow_id,
            query=query,
            arg=arg,
            run_id=run_id,
            result_type=result_type,
        )

    # ....................... #

    async def update_workflow(
        self,
        workflow_id: str,
        *,
        update: str,
        arg: BaseModel,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.update_workflow(
            workflow_id,
            update=update,
            arg=arg,
            run_id=run_id,
            result_type=result_type,
        )

    # ....................... #

    async def get_workflow_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.get_workflow_result(
            workflow_id,
            run_id=run_id,
            result_type=result_type,
        )

    # ....................... #

    async def describe_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> DurableWorkflowRunDescription:
        inner = await self._get_client()
        return await inner.describe_workflow(workflow_id, run_id=run_id)

    # ....................... #

    async def cancel_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> None:
        inner = await self._get_client()
        await inner.cancel_workflow(workflow_id, run_id=run_id)

    # ....................... #

    async def terminate_workflow(
        self,
        workflow_id: str,
        *,
        reason: str | None = None,
        run_id: str | None = None,
    ) -> None:
        inner = await self._get_client()
        await inner.terminate_workflow(
            workflow_id,
            reason=reason,
            run_id=run_id,
        )

    # ....................... #

    async def create_schedule(
        self,
        schedule_id: str,
        *,
        workflow_name: str,
        queue: str,
        arg: BaseModel,
        timing: DurableWorkflowScheduleTiming,
        workflow_id: str,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> None:
        inner = await self._get_client()
        await inner.create_schedule(
            schedule_id,
            workflow_name=workflow_name,
            queue=queue,
            arg=arg,
            timing=timing,
            workflow_id=workflow_id,
            trigger_immediately=trigger_immediately,
            note=note,
        )

    # ....................... #

    async def update_schedule(
        self,
        schedule_id: str,
        *,
        workflow_name: str,
        queue: str,
        arg: BaseModel | None,
        timing: DurableWorkflowScheduleTiming | None,
        workflow_id: str | None,
        note: str | None,
    ) -> None:
        inner = await self._get_client()
        await inner.update_schedule(
            schedule_id,
            workflow_name=workflow_name,
            queue=queue,
            arg=arg,
            timing=timing,
            workflow_id=workflow_id,
            note=note,
        )

    # ....................... #

    async def delete_schedule(self, schedule_id: str) -> None:
        inner = await self._get_client()
        await inner.delete_schedule(schedule_id)

    # ....................... #

    async def pause_schedule(
        self,
        schedule_id: str,
        *,
        note: str | None = None,
    ) -> None:
        inner = await self._get_client()
        await inner.pause_schedule(schedule_id, note=note)

    # ....................... #

    async def unpause_schedule(
        self,
        schedule_id: str,
        *,
        note: str | None = None,
    ) -> None:
        inner = await self._get_client()
        await inner.unpause_schedule(schedule_id, note=note)

    # ....................... #

    async def trigger_schedule(self, schedule_id: str) -> None:
        inner = await self._get_client()
        await inner.trigger_schedule(schedule_id)

    # ....................... #

    async def describe_schedule(
        self,
        schedule_id: str,
    ) -> DurableWorkflowScheduleDescription:
        inner = await self._get_client()
        return await inner.describe_schedule(schedule_id)

    # ....................... #

    async def list_schedules(
        self,
        *,
        workflow_name: str | None = None,
        limit: int | None = None,
        next_page_token: str | None = None,
        schedule_id_prefix: str | None = None,
    ) -> TemporalScheduleListPage:
        inner = await self._get_client()
        return await inner.list_schedules(
            workflow_name=workflow_name,
            limit=limit,
            next_page_token=next_page_token,
            schedule_id_prefix=schedule_id_prefix,
        )
