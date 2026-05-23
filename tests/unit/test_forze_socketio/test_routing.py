"""Unit tests for :mod:`forze_socketio.routing`."""

from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.registry import OperationRegistry
from forze.base.errors import CoreError
from forze.application.execution import make_registry_operation_resolver
from forze_socketio.routing import (
    ForzeSocketIOAdapter,
    SocketIONamespaceRouter,
    SocketIORequest,
)

# ----------------------- #


class StubSocketIOServer:
    def __init__(self) -> None:
        self.handlers: dict[tuple[str, str], Any] = {}

    def on(
        self,
        event: str,
        handler: Any = None,
        namespace: str | None = None,
    ) -> Any:
        namespace = namespace or "/"

        if handler is None:

            def decorator(func: Any) -> Any:
                self.handlers[(namespace, event)] = func
                return func

            return decorator

        self.handlers[(namespace, event)] = handler
        return handler


class EchoPayload(BaseModel):
    text: str


class EchoAck(BaseModel):
    echoed: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class EchoHandler(Handler[EchoPayload, EchoAck]):
    async def __call__(self, args: EchoPayload) -> EchoAck:
        return EchoAck(echoed=args.text)


def _frozen_echo_registry() -> OperationRegistry:
    return OperationRegistry(
        handlers={"chat.echo": lambda _ctx: EchoHandler()},
    ).freeze()


class TestSocketIORouting:
    @pytest.mark.asyncio
    async def test_event_dispatch_validates_and_runs_handler(self) -> None:
        sio = StubSocketIOServer()
        registry = _frozen_echo_registry()
        request_log: list[SocketIORequest] = []

        def context_factory(request: SocketIORequest) -> ExecutionContext:
            request_log.append(request)
            return ExecutionContext(deps=Deps())

        router = SocketIONamespaceRouter(namespace="/chat").command(
            event="echo",
            operation="chat.echo",
            payload_type=EchoPayload,
            ack_type=EchoAck,
        )
        adapter = ForzeSocketIOAdapter(
            sio=sio,  # pyright: ignore[reportArgumentType]
            context_factory=context_factory,
            operation_resolver=make_registry_operation_resolver(registry),
        )
        adapter.include_router(router)

        handler = sio.handlers[("/chat", "echo")]
        result = await handler("sid-1", {"text": "hello"})

        assert result == {"echoed": "hello"}
        assert request_log == [
            SocketIORequest(sid="sid-1", namespace="/chat", event="echo")
        ]

    def test_router_raises_on_duplicate_event(self) -> None:
        router = SocketIONamespaceRouter(namespace="/chat").command(
            event="echo",
            operation="chat.echo",
            payload_type=EchoPayload,
            ack_type=EchoAck,
        )

        with pytest.raises(CoreError, match="already registered"):
            router.command(
                event="echo",
                operation="chat.echo",
                payload_type=EchoPayload,
                ack_type=EchoAck,
            )


@pytest.mark.asyncio
async def test_event_dispatch_without_ack_type_returns_raw_value() -> None:
    sio = StubSocketIOServer()
    registry = _frozen_echo_registry()

    def context_factory(request: SocketIORequest) -> ExecutionContext:
        return ExecutionContext(deps=Deps())

    router = SocketIONamespaceRouter(namespace="/chat").command(
        event="echo",
        operation="chat.echo",
        payload_type=EchoPayload,
        ack_type=None,
    )
    adapter = ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=context_factory,
        operation_resolver=make_registry_operation_resolver(registry),
    )
    adapter.include_router(router)

    handler = sio.handlers[("/chat", "echo")]
    result = await handler("sid-1", {"text": "hello"})

    assert isinstance(result, EchoAck)
    assert result.echoed == "hello"
