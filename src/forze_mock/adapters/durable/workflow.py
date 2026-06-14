"""In-memory durable workflow command and query adapters."""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandPort,
    DurableWorkflowHandle,
    DurableWorkflowQueryPort,
    DurableWorkflowQuerySpec,
    DurableWorkflowRunDescription,
    DurableWorkflowRunStatus,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
    DurableWorkflowUpdateSpec,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #


def _workflow_runs(state: MockState, namespace: str) -> dict[str, Any]:
    with state.lock:
        return state.durable_workflows.setdefault(namespace, {})


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDurableWorkflowCommandAdapter[In: BaseModel, Out: BaseModel](
    MockTenancyMixin,
    DurableWorkflowCommandPort[In, Out],
):
    spec: DurableWorkflowSpec[In, Out]
    state: MockState

    def _runs(self) -> dict[str, Any]:
        # Tenant-partition the run store (mirrors Temporal's per-tenant task queue);
        # fails closed when tenant_aware with no bound tenant.
        return _workflow_runs(
            self.state, self._partitioned_namespace(str(self.spec.name))
        )

    async def start(
        self,
        args: In,
        *,
        workflow_id: str | None = None,
        raise_on_already_started: bool = True,
    ) -> DurableWorkflowHandle:
        wid = workflow_id or str(uuid7())
        run_id = str(uuid7())
        with self.state.lock:
            runs = self._runs()
            if wid in runs and raise_on_already_started:
                raise exc.conflict(f"Workflow {wid!r} already started")
            runs[wid] = {
                "run_id": run_id,
                "args": args.model_dump(mode="json"),
                "status": DurableWorkflowRunStatus.RUNNING,
                "result": None,
                "signals": [],
            }
        return DurableWorkflowHandle(workflow_id=wid, run_id=run_id)

    async def signal[S: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        signal: DurableWorkflowSignalSpec[S],
        args: S,
    ) -> None:
        with self.state.lock:
            run = self._runs().get(handle.workflow_id)
            if run is None:
                raise exc.not_found(f"Workflow {handle.workflow_id!r} not found")
            run["signals"].append((signal.name, args.model_dump(mode="json")))

    async def update[U: BaseModel, Res: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        update: DurableWorkflowUpdateSpec[U, Res],
        args: U,
    ) -> Res:
        _ = handle, update, args
        raise exc.internal("Mock workflow update is not implemented")

    async def cancel(self, handle: DurableWorkflowHandle) -> None:
        with self.state.lock:
            run = self._runs().get(handle.workflow_id)
            if run is None:
                return
            run["status"] = DurableWorkflowRunStatus.CANCELLED

    async def terminate(
        self,
        handle: DurableWorkflowHandle,
        *,
        reason: str | None = None,
    ) -> None:
        _ = reason
        with self.state.lock:
            run = self._runs().get(handle.workflow_id)
            if run is None:
                return
            run["status"] = DurableWorkflowRunStatus.TERMINATED


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDurableWorkflowQueryAdapter[In: BaseModel, Out: BaseModel](
    MockTenancyMixin,
    DurableWorkflowQueryPort[In, Out],
):
    spec: DurableWorkflowSpec[In, Out]
    state: MockState

    def _runs(self) -> dict[str, Any]:
        return _workflow_runs(
            self.state, self._partitioned_namespace(str(self.spec.name))
        )

    async def query[Q: BaseModel, Res: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        query: DurableWorkflowQuerySpec[Q, Res],
        args: Q,
    ) -> Res:
        _ = handle, query, args
        raise exc.internal("Mock workflow query is not implemented")

    async def result(self, handle: DurableWorkflowHandle) -> Out:
        with self.state.lock:
            run = self._runs().get(handle.workflow_id)
            if run is None:
                raise exc.not_found(f"Workflow {handle.workflow_id!r} not found")
            raw = run.get("result")
            if raw is None:
                raise exc.not_found("Workflow result not available")
            ret_type = self.spec.run.return_type
            if ret_type is None:
                raise exc.internal("Workflow has no return type")
            return ret_type.model_validate(raw)

    async def describe(
        self,
        handle: DurableWorkflowHandle,
    ) -> DurableWorkflowRunDescription:
        with self.state.lock:
            run = self._runs().get(handle.workflow_id)
            if run is None:
                raise exc.not_found(f"Workflow {handle.workflow_id!r} not found")
            return DurableWorkflowRunDescription(
                workflow_id=handle.workflow_id,
                run_id=str(run["run_id"]),
                workflow_name=self.spec.name,
                status=run["status"],
                started_at=utcnow(),
            )
