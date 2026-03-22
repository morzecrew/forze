"""Unit tests for :mod:`forze_socketio.routing`."""

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.execution import (
    Deps,
    ExecutionContext,
    Usecase,
    UsecasePlan,
    UsecaseRegistry,
)
from forze.base.errors import CoreError
from forze_socketio.routing import (
    ForzeSocketIOAdapter,
    SocketIONamespaceRouter,
    SocketIORequest,
    make_registry_usecase_resolver,
)

# ----------------------- #


class StubSocketIOServer:
    """Minimal Socket.IO server stub for handler registration tests."""

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
    """Inbound payload model."""

    text: str


class EchoAck(BaseModel):
    """Acknowledgement payload model."""

    echoed: str


class EchoUsecase(Usecase[EchoPayload, EchoAck]):
    """Simple echo usecase for event dispatch tests."""

    async def main(self, args: EchoPayload) -> EchoAck:
        return EchoAck(echoed=args.text)


class TestSocketIORouting:
    """Tests for namespace routing and usecase dispatch."""

    @pytest.mark.asyncio
    async def test_event_dispatch_validates_and_runs_usecase_with_plan(self) -> None:
        sio = StubSocketIOServer()
        registry = UsecaseRegistry().register(
            "chat.echo", lambda ctx: EchoUsecase(ctx=ctx)
        )

        request_log: list[SocketIORequest] = []
        guard_log: list[str] = []

        async def guard(args: EchoPayload) -> None:
            guard_log.append(args.text)

        def guard_factory(ctx: ExecutionContext):
            return guard

        plan = UsecasePlan().before("chat.echo", guard_factory, priority=10)
        registry = registry.extend_plan(plan)
        registry.finalize("socketio", inplace=True)
        resolver = make_registry_usecase_resolver(registry)

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
            usecase_resolver=resolver,
        )
        adapter.include_router(router)

        handler = sio.handlers[("/chat", "echo")]
        result = await handler("sid-1", {"text": "hello"})

        assert result == {"echoed": "hello"}
        assert guard_log == ["hello"]
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

    def test_adapter_raises_on_duplicate_namespace(self) -> None:
        sio = StubSocketIOServer()

        def context_factory(request: SocketIORequest) -> ExecutionContext:
            return ExecutionContext(deps=Deps())

        registry = UsecaseRegistry().register(
            "chat.echo", lambda ctx: EchoUsecase(ctx=ctx)
        )
        resolver = make_registry_usecase_resolver(registry)

        adapter = ForzeSocketIOAdapter(
            sio=sio,  # pyright: ignore[reportArgumentType]
            context_factory=context_factory,
            usecase_resolver=resolver,
        )
        adapter.include_router(
            SocketIONamespaceRouter(namespace="/chat").command(
                event="echo",
                operation="chat.echo",
                payload_type=EchoPayload,
                ack_type=EchoAck,
            )
        )

        with pytest.raises(CoreError, match="already attached"):
            adapter.include_router(
                SocketIONamespaceRouter(namespace="/chat").command(
                    event="ping",
                    operation="chat.echo",
                    payload_type=EchoPayload,
                    ack_type=EchoAck,
                )
            )


@pytest.mark.asyncio
async def test_event_dispatch_without_ack_type_returns_raw_value() -> None:
    sio = StubSocketIOServer()
    registry = UsecaseRegistry().register("chat.echo", lambda ctx: EchoUsecase(ctx=ctx))
    registry.finalize("socketio", inplace=True)
    resolver = make_registry_usecase_resolver(registry)

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
        usecase_resolver=resolver,
    )
    adapter.include_router(router)

    handler = sio.handlers[("/chat", "echo")]
    result = await handler("sid-1", {"text": "hello"})

    # Should return raw EchoAck object since ack_type is None
    assert isinstance(result, EchoAck)
    assert result.echoed == "hello"


def test_adapter_routers_property() -> None:
    sio = StubSocketIOServer()

    def context_factory(request: SocketIORequest) -> ExecutionContext:
        return ExecutionContext(deps=Deps())

    adapter = ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=context_factory,
        usecase_resolver=make_registry_usecase_resolver(UsecaseRegistry()),
    )

    router1 = SocketIONamespaceRouter(namespace="/chat1")
    router2 = SocketIONamespaceRouter(namespace="/chat2")

    adapter.include_router(router1)
    adapter.include_router(router2)

    assert adapter.routers == (router1, router2)


def test_adapter_include_routers() -> None:
    sio = StubSocketIOServer()

    def context_factory(request: SocketIORequest) -> ExecutionContext:
        return ExecutionContext(deps=Deps())

    adapter = ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=context_factory,
        usecase_resolver=make_registry_usecase_resolver(UsecaseRegistry()),
    )

    router1 = SocketIONamespaceRouter(namespace="/chat1")
    router2 = SocketIONamespaceRouter(namespace="/chat2")

    adapter.include_routers(router1, router2)

    assert adapter.routers == (router1, router2)


@pytest.mark.asyncio
async def test_async_context_factory() -> None:
    sio = StubSocketIOServer()
    registry = UsecaseRegistry().register("chat.echo", lambda ctx: EchoUsecase(ctx=ctx))
    registry.finalize("socketio", inplace=True)
    resolver = make_registry_usecase_resolver(registry)

    async def async_context_factory(request: SocketIORequest) -> ExecutionContext:
        return ExecutionContext(deps=Deps())

    router = SocketIONamespaceRouter(namespace="/chat").command(
        event="echo",
        operation="chat.echo",
        payload_type=EchoPayload,
        ack_type=EchoAck,
    )
    adapter = ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=async_context_factory,
        usecase_resolver=resolver,
    )
    adapter.include_router(router)

    handler = sio.handlers[("/chat", "echo")]
    result = await handler("sid-1", {"text": "hello"})

    assert result == {"echoed": "hello"}
