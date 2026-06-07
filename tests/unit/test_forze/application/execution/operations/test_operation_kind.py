"""Operation-level CQRS: a QUERY op runs read-only and cannot acquire a command port."""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import ExecutionContext, OperationKind
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from pydantic import BaseModel

from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class Thing(Document):
    name: str = "x"


class ThingCreate(CreateDocumentCmd):
    name: str = "x"


class ThingUpdate(BaseDTO):
    name: str | None = None


class ThingRead(ReadDocument):
    name: str


SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(
        domain=Thing, create_cmd=ThingCreate, update_cmd=ThingUpdate
    ),
)


class _EventPayload(BaseModel):
    note: str


OUTBOX_SPEC = OutboxSpec(name="things-events", codec=PydanticModelCodec(_EventPayload))


@attrs.define(slots=True)
class _AcquireDocumentCommand(Handler[None, str]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> str:
        self.ctx.document.command(SPEC)
        return "wrote"


@attrs.define(slots=True)
class _AcquireDocumentQuery(Handler[None, str]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> str:
        self.ctx.document.query(SPEC)
        return "read"


@attrs.define(slots=True)
class _AcquireOutboxCommand(Handler[None, str]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> str:
        self.ctx.outbox.command(OUTBOX_SPEC)
        return "staged"


@attrs.define(slots=True)
class _ReadFlag(Handler[None, bool]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> bool:
        return self.ctx.inv_ctx.is_read_only()


@attrs.define(slots=True)
class _Noop(Handler[None, None]):
    async def __call__(self, _args: None) -> None:
        return None


def _frozen(op: str, factory, *, query: bool):
    binder = OperationRegistry(handlers={op: factory}).bind(op)
    if query:
        binder = binder.as_query()
    return binder.finish().freeze()


# ....................... #


class TestOperationKind:
    async def test_query_op_cannot_acquire_a_command_port(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        reg = _frozen("q", lambda c: _AcquireDocumentCommand(ctx=c), query=True)

        with pytest.raises(CoreException) as ei:
            await run_operation(reg, "q", None, ctx)

        assert ei.value.kind is ExceptionKind.PRECONDITION

    async def test_query_op_can_acquire_a_query_port(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        reg = _frozen("q", lambda c: _AcquireDocumentQuery(ctx=c), query=True)

        assert await run_operation(reg, "q", None, ctx) == "read"

    async def test_command_op_writes_normally(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        reg = _frozen("c", lambda c: _AcquireDocumentCommand(ctx=c), query=False)

        assert await run_operation(reg, "c", None, ctx) == "wrote"

    async def test_guard_is_not_document_specific(self) -> None:
        # The guard lives at the shared ConvenientDeps layer — outbox command too.
        ctx = context_from_modules(MockDepsModule())
        reg = _frozen("q", lambda c: _AcquireOutboxCommand(ctx=c), query=True)

        with pytest.raises(CoreException) as ei:
            await run_operation(reg, "q", None, ctx)
        assert ei.value.kind is ExceptionKind.PRECONDITION

    async def test_read_only_flag_is_scoped_to_the_query_op(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        q = _frozen("q", lambda c: _ReadFlag(ctx=c), query=True)
        c = _frozen("c", lambda c: _ReadFlag(ctx=c), query=False)

        assert ctx.inv_ctx.is_read_only() is False  # before
        assert await run_operation(q, "q", None, ctx) is True  # inside the query op
        assert await run_operation(c, "c", None, ctx) is False  # inside the command op
        assert ctx.inv_ctx.is_read_only() is False  # after — ContextVar reset

    def test_as_query_sets_kind_on_the_resolved_plan(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        q = _frozen("q", lambda c: _ReadFlag(ctx=c), query=True)
        c = _frozen("c", lambda c: _ReadFlag(ctx=c), query=False)

        assert q.resolve("q", ctx).plan.kind is OperationKind.QUERY
        assert c.resolve("c", ctx).plan.kind is OperationKind.COMMAND  # default


class TestReadOnlyTransaction:
    def _txn_op(self, op: str, *, query: bool):
        binder = OperationRegistry(handlers={op: lambda _c: _Noop()}).bind(op)
        if query:
            binder = binder.as_query()
        return binder.bind_tx().set_route("mock").finish(deep=True).freeze()

    async def test_query_op_opens_a_read_only_transaction(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))

        await run_operation(self._txn_op("q", query=True), "q", None, ctx)

        assert state.tx_read_only_calls == [True]

    async def test_command_op_opens_a_read_write_transaction(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))

        await run_operation(self._txn_op("c", query=False), "c", None, ctx)

        assert state.tx_read_only_calls == [False]

    async def test_direct_scope_defaults_to_read_write(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))

        async with ctx.tx_ctx.scope("mock"):
            pass

        assert state.tx_read_only_calls == [False]
