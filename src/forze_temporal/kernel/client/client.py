from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

import base64
from typing import Any, final

import attrs
from pydantic import BaseModel
from temporalio.client import (
    Client,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleUpdate,
    ScheduleUpdateInput,
    WorkflowHandle,
)
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError

from forze.application.contracts.durable.workflow import (
    DurableWorkflowRunDescription,
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleTiming,
)
from forze.base.exceptions import exc
from forze.base.primitives import GuardedLifecycle

from .port import TemporalClientPort
from .schedule_mapping import (
    build_schedule,
    description_from_list_entry,
    description_from_temporal,
    timing_to_schedule_spec,
)
from .workflow_mapping import description_from_temporal_execution
from .schedule_types import TemporalScheduleListPage
from .value_objects import TemporalConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class TemporalClient(TemporalClientPort):
    """Low level client for temporal.io."""

    __client: Client | None = attrs.field(default=None, init=False)
    __lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        host: str,
        *,
        config: TemporalConfig = TemporalConfig(),
    ) -> None:
        async def setup() -> None:
            # Only forward optional security kwargs when set so the default
            # connect call stays byte-identical to previous releases.
            connect_kwargs: dict[str, Any] = {}

            if config.tls:
                connect_kwargs["tls"] = config.tls

            if config.api_key is not None:
                connect_kwargs["api_key"] = config.api_key.get_secret_value()

            if config.rpc_metadata:
                connect_kwargs["rpc_metadata"] = dict(config.rpc_metadata)

            self.__client = await Client.connect(
                host,
                namespace=config.namespace,
                lazy=config.lazy,
                data_converter=config.data_converter or pydantic_data_converter,
                interceptors=config.interceptors or [],
                **connect_kwargs,
            )

        await self.__lifecycle.initialize(
            setup,
            ready=lambda: self.__client is not None,
        )

    # ....................... #

    async def close(self) -> None:
        async def teardown() -> None:
            self.__client = None

        await self.__lifecycle.close(teardown)

    # ....................... #

    def __require_client(self) -> Client:
        if self.__client is None:
            raise exc.internal("Temporal client is not initialized")

        return self.__client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        try:
            await self.__require_client().count_workflows()
            return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #
    # Main API

    async def describe_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> DurableWorkflowRunDescription:
        h = self.get_workflow_handle(workflow_id, run_id=run_id)

        desc = await h.describe()

        return description_from_temporal_execution(desc)

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
        c = self.__require_client()

        try:
            handle = await c.start_workflow(
                workflow=name,
                id=workflow_id,
                task_queue=queue,
                arg=arg,
            )

        except WorkflowAlreadyStartedError as e:
            # log e
            if raise_on_already_started:
                raise e

            handle = c.get_workflow_handle(workflow_id)

        return handle

    # ....................... #

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None
    ) -> WorkflowHandle[Any, Any]:
        c = self.__require_client()

        return c.get_workflow_handle(workflow_id, run_id=run_id)

    # ....................... #

    async def signal_workflow(
        self,
        workflow_id: str,
        *,
        signal: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> None:
        h = self.get_workflow_handle(workflow_id, run_id=run_id)

        await h.signal(signal=signal, arg=arg)

    # ....................... #

    async def query_workflow(
        self,
        workflow_id: str,
        *,
        query: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Any:
        h = self.get_workflow_handle(workflow_id, run_id=run_id)

        return await h.query(query=query, arg=arg)

    # ....................... #

    async def update_workflow(
        self,
        workflow_id: str,
        *,
        update: str,
        arg: BaseModel,
        run_id: str | None = None,
    ) -> Any:
        h = self.get_workflow_handle(workflow_id, run_id=run_id)

        return await h.execute_update(update=update, arg=arg)  # type: ignore[misc]

    # ....................... #

    async def get_workflow_result(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> Any:
        h = self.get_workflow_handle(workflow_id, run_id=run_id)

        return await h.result()

    # ....................... #

    async def cancel_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> None:
        h = self.get_workflow_handle(workflow_id, run_id=run_id)

        await h.cancel()

    # ....................... #

    async def terminate_workflow(
        self,
        workflow_id: str,
        *,
        reason: str | None = None,
        run_id: str | None = None,
    ) -> None:
        h = self.get_workflow_handle(workflow_id, run_id=run_id)

        await h.terminate(reason=reason)

    # ....................... #
    # Schedules

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
        c = self.__require_client()
        schedule = build_schedule(
            workflow_name=workflow_name,
            queue=queue,
            arg=arg,
            workflow_id=workflow_id,
            timing=timing,
            note=note,
        )

        try:
            await c.create_schedule(
                schedule_id,
                schedule,
                trigger_immediately=trigger_immediately,
            )

        except ScheduleAlreadyRunningError as e:
            raise exc.conflict(f"Schedule {schedule_id!r} already exists") from e

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
        c = self.__require_client()
        handle = c.get_schedule_handle(schedule_id)

        def updater(input: ScheduleUpdateInput) -> ScheduleUpdate:
            sched = input.description.schedule
            action = sched.action

            if not isinstance(action, ScheduleActionStartWorkflow):
                raise exc.internal("Schedule action is not a start-workflow action")

            if arg is not None:
                action.args = [arg]

            if workflow_id is not None:
                action.id = workflow_id

            action.workflow = workflow_name
            action.task_queue = queue

            if timing is not None:
                sched.spec = timing_to_schedule_spec(timing)

            if note is not None:
                sched.state.note = note

            return ScheduleUpdate(schedule=sched)

        await handle.update(updater)

    # ....................... #

    async def delete_schedule(self, schedule_id: str) -> None:
        c = self.__require_client()
        handle = c.get_schedule_handle(schedule_id)
        await handle.delete()

    # ....................... #

    async def pause_schedule(
        self,
        schedule_id: str,
        *,
        note: str | None = None,
    ) -> None:
        c = self.__require_client()
        handle = c.get_schedule_handle(schedule_id)
        await handle.pause(note=note or "")

    # ....................... #

    async def unpause_schedule(
        self,
        schedule_id: str,
        *,
        note: str | None = None,
    ) -> None:
        c = self.__require_client()
        handle = c.get_schedule_handle(schedule_id)
        await handle.unpause(note=note or "")

    # ....................... #

    async def trigger_schedule(self, schedule_id: str) -> None:
        c = self.__require_client()
        handle = c.get_schedule_handle(schedule_id)
        await handle.trigger()

    # ....................... #

    async def describe_schedule(
        self, schedule_id: str
    ) -> DurableWorkflowScheduleDescription:
        c = self.__require_client()

        handle = c.get_schedule_handle(schedule_id)
        desc = await handle.describe()

        action = desc.schedule.action
        workflow_name = (
            action.workflow if isinstance(action, ScheduleActionStartWorkflow) else ""
        )
        return description_from_temporal(desc, workflow_name=workflow_name)

    # ....................... #

    async def list_schedules(
        self,
        *,
        workflow_name: str | None = None,
        limit: int | None = None,
        next_page_token: str | None = None,
    ) -> TemporalScheduleListPage:
        c = self.__require_client()
        page_size = limit if limit is not None else 100

        token_bytes = (
            base64.urlsafe_b64decode(next_page_token.encode())
            if next_page_token
            else None
        )

        descriptions: list[DurableWorkflowScheduleDescription] = []

        iterator = await c.list_schedules(
            page_size=page_size,
            next_page_token=token_bytes,
        )

        async for entry in iterator:
            mapped = description_from_list_entry(entry)

            if mapped is None:
                continue

            if workflow_name is not None and mapped.workflow_name != workflow_name:
                continue

            descriptions.append(mapped)

            if limit is not None and len(descriptions) >= limit:
                break

        next_token: str | None = None

        if iterator.next_page_token:
            next_token = base64.urlsafe_b64encode(iterator.next_page_token).decode()

        return TemporalScheduleListPage(
            descriptions=tuple(descriptions),
            next_page_token=next_token,
        )
