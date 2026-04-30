"""Temporal client that resolves a server address per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

import attrs
from pydantic import BaseModel
from temporalio.client import WorkflowHandle

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError

from .client import TemporalClient, TemporalConfig

# ----------------------- #


@attrs.define(slots=True)
class RoutedTemporalClient:
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

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _clients: OrderedDict[UUID, TemporalClient] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise CoreError("max_cached_tenants must be at least 1")

    # ....................... #

    def _get_secret_ref(self, tenant_id: UUID) -> SecretRef:
        if callable(self.secret_ref_for_tenant):
            return self.secret_ref_for_tenant(tenant_id)

        return self.secret_ref_for_tenant[tenant_id]

    # ....................... #

    async def startup(self) -> None:
        self._started = True

    # ....................... #

    async def close(self) -> None:
        async with self._lock:
            to_close = list(self._clients.values())
            self._clients.clear()

        for c in to_close:
            await c.close()

        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        async with self._lock:
            client = self._clients.pop(tenant_id, None)

        if client is not None:
            await client.close()

    # ....................... #

    def _require_tenant_id(self) -> UUID:
        tid = self.tenant_provider()

        if tid is None:
            raise CoreError(
                "Tenant ID is required for routed Temporal access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    async def _get_client(self) -> TemporalClient:
        if not self._started:
            raise InfrastructureError("Routed Temporal client is not started")

        tid = self._require_tenant_id()

        async with self._lock:
            if tid in self._clients:
                client = self._clients[tid]
                self._clients.move_to_end(tid)
                return client

            ref = self._get_secret_ref(tid)

            try:
                host = await self.secrets.resolve_str(ref)

            except SecretNotFoundError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve Temporal secret for tenant {tid}: {e}",
                ) from e

            client = TemporalClient()
            await client.initialize(host, config=self.connection_config)
            self._clients[tid] = client
            self._clients.move_to_end(tid)

            while len(self._clients) > self.max_cached_tenants:
                _, old = self._clients.popitem(last=False)
                await old.close()

            return client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

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

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None
    ) -> WorkflowHandle[Any, Any]:
        if not self._started:
            raise InfrastructureError("Routed Temporal client is not started")

        tid = self._require_tenant_id()
        inner = self._clients.get(tid)

        if inner is None:
            raise InfrastructureError(
                "No Temporal client for this tenant in cache; call an async method "
                "(e.g. :meth:`start_workflow` or :meth:`health`) first to connect.",
            )

        return inner.get_workflow_handle(workflow_id, run_id=run_id)

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

    async def query_workflow(
        self,
        workflow_id: str,
        *,
        query: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.query_workflow(
            workflow_id,
            query=query,
            arg=arg,
            run_id=run_id,
        )

    async def update_workflow(
        self,
        workflow_id: str,
        *,
        update: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.update_workflow(
            workflow_id,
            update=update,
            arg=arg,
            run_id=run_id,
        )

    async def get_workflow_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> Any:
        inner = await self._get_client()
        return await inner.get_workflow_result(workflow_id, run_id=run_id)

    async def cancel_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> None:
        inner = await self._get_client()
        await inner.cancel_workflow(workflow_id, run_id=run_id)

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
