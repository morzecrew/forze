from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from typing import Any, final

import attrs
from pydantic import BaseModel
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError

from forze.base.errors import InfrastructureError

from .port import TemporalClientPort
from .value_objects import TemporalConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class TemporalClient(TemporalClientPort):
    """Low level client for temporal.io."""

    __client: Client | None = attrs.field(default=None, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        host: str,
        *,
        config: TemporalConfig = TemporalConfig(),
    ) -> None:
        if self.__client is not None:
            # log
            return

        self.__client = await Client.connect(
            host,
            namespace=config.namespace,
            lazy=config.lazy,
            # Default values (not configurable)
            data_converter=pydantic_data_converter,
            interceptors=config.interceptors or [],
        )

    # ....................... #

    async def close(self) -> None:
        if self.__client is not None:
            self.__client = None

    # ....................... #

    def __require_client(self) -> Client:
        if self.__client is None:
            raise InfrastructureError("Temporal client is not initialized")

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

    #! TODO: add `async def describe_workflow(self, workflow_id: str, *, run_id: str | None = None)`

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
