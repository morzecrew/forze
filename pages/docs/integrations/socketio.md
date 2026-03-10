# Socket.IO Integration

This guide shows how to execute Forze usecases through Socket.IO events with
`forze_socketio`.

## Prerequisites

- `forze[socketio]` installed
- an `ExecutionContext` factory for each inbound event
- usecases registered in a `UsecaseRegistry`

## What `forze_socketio` gives you

- Typed command event routing (`SocketIONamespaceRouter`)
- Usecase dispatch through `ExecutionContext` and middleware plans
- Typed outbound event emission (`SocketIOEventEmitter`)
- Optional Redis backplane setup for distributed deployment

## Build a server

Use `build_socketio_server` to create an async server and optionally enable
the official Redis manager for clustering:

    :::python
    from forze_socketio import build_socketio_server

    sio = build_socketio_server(
        async_mode="asgi",
        cors_allowed_origins="*",
        redis_url="redis://localhost:6379/0",  # optional
    )

## Route command events to usecases

Define namespace routes as typed command events. Each event validates input,
builds `ExecutionContext`, resolves the usecase, executes it, and returns a
validated acknowledgement payload.

    :::python
    from forze.application.execution import ExecutionContext, UsecaseRegistry
    from forze.application.execution import Deps
    from forze_socketio import (
        ForzeSocketIOAdapter,
        SocketIONamespaceRouter,
        make_registry_usecase_resolver,
    )
    from myapp.dto import JoinRoomCmd, JoinRoomAck
    from myapp.usecases import build_join_room_usecase

    registry = UsecaseRegistry().register(
        "chat.join",
        lambda ctx: build_join_room_usecase(ctx),
    )

    router = SocketIONamespaceRouter(namespace="/chat").command(
        event="join",
        operation="chat.join",
        payload_type=JoinRoomCmd,
        ack_type=JoinRoomAck,
    )

    def context_factory(request) -> ExecutionContext:
        # Build a request-scoped context; request contains sid/namespace/event.
        return ExecutionContext(deps=Deps())

    adapter = ForzeSocketIOAdapter(
        sio=sio,
        context_factory=context_factory,
        usecase_resolver=make_registry_usecase_resolver(registry),
    )
    adapter.include_router(router)

## Emit typed server events

Use `SocketIOEventEmitter` to validate payloads and hide transport details from
application code or workers:

    :::python
    from forze_socketio import SocketIOEventEmitter, SocketIOServerEvent
    from myapp.dto import ProgressEvent

    progress_event = SocketIOServerEvent(
        event="task.progress",
        namespace="/tasks",
        payload_type=ProgressEvent,
    )

    emitter = SocketIOEventEmitter(sio=sio)
    await emitter.emit(progress_event, {"done": 3, "total": 10}, room="task:42")

## ASGI integration

Wrap the Socket.IO server as an ASGI app when needed:

    :::python
    from forze_socketio import build_socketio_asgi_app

    app = build_socketio_asgi_app(sio, other_asgi_app=fastapi_app)
