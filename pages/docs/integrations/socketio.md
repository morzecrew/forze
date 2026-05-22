# Socket.IO Integration

## What this integration provides

Expose typed realtime events while keeping event handlers wired to application handlers instead of transport-specific business logic.

## When to use it

Use this when clients need bidirectional updates, command events, or server-emitted events over Socket.IO.

## Standard setup checklist

1. Install the matching optional extra.
2. Create the integration client or module configuration.
3. Register the module in `DepsPlan` with routes that match your specs.
4. Add lifecycle steps when the integration opens network connections.
5. Resolve ports from `ExecutionContext`; do not import adapters in handlers.


`forze_socketio` connects Forze handlers to real-time Socket.IO events. It provides typed command routing, operation dispatch through `ExecutionContext`, typed server event emission, and optional Redis backplane support for distributed deployments.

## Installation

    :::bash
    uv add 'forze[socketio]'

## Building a server

Create an async Socket.IO server with `build_socketio_server`. Optionally enable the Redis manager for multi-process clustering:

    :::python
    from forze_socketio import build_socketio_server

    sio = build_socketio_server(
        async_mode="asgi",
        cors_allowed_origins="*",
        redis_url="redis://localhost:6379/0",  # optional, for distributed setups
    )

Without `redis_url`, the server uses in-memory transport (single-process only).

## Command event routing

Define namespace routes as typed command events. Each event validates input against a Pydantic model, builds an `ExecutionContext`, resolves the operation, executes the handler, and returns a validated acknowledgement payload.

### Define DTOs

    :::python
    from pydantic import BaseModel


    class JoinRoomCmd(BaseModel):
        room: str
        user_id: str


    class JoinRoomAck(BaseModel):
        room: str
        joined: bool

### Create a handler

    :::python
    from forze.application.contracts.execution import Handler


    class JoinRoom(Handler[JoinRoomCmd, JoinRoomAck]):
        async def __call__(self, args: JoinRoomCmd) -> JoinRoomAck:
            return JoinRoomAck(room=args.room, joined=True)

### Register and route

    :::python
    from forze.application.execution import Deps, ExecutionContext, OperationRegistry
    from forze_socketio import (
        ForzeSocketIOAdapter,
        SocketIONamespaceRouter,
        make_registry_operation_resolver,
    )

    registry = (
        OperationRegistry(handlers={"chat.join": lambda _ctx: JoinRoom()})
        .freeze()
    )

    router = SocketIONamespaceRouter(namespace="/chat").command(
        event="join",
        operation="chat.join",
        payload_type=JoinRoomCmd,
        ack_type=JoinRoomAck,
    )


    def context_factory(request) -> ExecutionContext:
        return ExecutionContext(deps=Deps())


    adapter = ForzeSocketIOAdapter(
        sio=sio,
        context_factory=context_factory,
        usecase_resolver=make_registry_operation_resolver(registry),
    )
    adapter.include_router(router)

`make_registry_usecase_resolver` is a deprecated alias for `make_registry_operation_resolver`.

The adapter handles:

1. Deserializing the incoming event payload into `JoinRoomCmd`
2. Calling `context_factory` to create a request-scoped `ExecutionContext`
3. Resolving `chat.join` from the frozen registry with the context
4. Executing the handler and serializing the result as `JoinRoomAck`
5. Returning the acknowledgement to the client

### Multiple namespaces

Register multiple routers for different namespaces:

    :::python
    chat_router = SocketIONamespaceRouter(namespace="/chat").command(
        event="join", operation="chat.join",
        payload_type=JoinRoomCmd, ack_type=JoinRoomAck,
    ).command(
        event="leave", operation="chat.leave",
        payload_type=LeaveRoomCmd, ack_type=LeaveRoomAck,
    )

    tasks_router = SocketIONamespaceRouter(namespace="/tasks").command(
        event="start", operation="tasks.start",
        payload_type=StartTaskCmd, ack_type=StartTaskAck,
    )

    adapter.include_router(chat_router)
    adapter.include_router(tasks_router)
