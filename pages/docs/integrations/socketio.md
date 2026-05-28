# Socket.IO Integration

`forze_socketio` routes typed Socket.IO command events to frozen `OperationRegistry` handlers and emits validated server events. Business logic stays in handlers and ports; this package handles payload validation, `ExecutionContext` creation, and acknowledgements.

| Topic | Details |
|------|---------|
| What it provides | Namespace routers, `ForzeSocketIOAdapter`, `make_registry_operation_resolver`, server/ASGI builders, typed emitters. |
| When to use it | Realtime command/ack flows, multi-namespace APIs, optional Redis backplane for multi-process deployments. |
| Registry | **Frozen** registry required — same as FastAPI `attach_*` helpers. |

## Installation

```bash
uv add 'forze[socketio]'
```

## Server

```python
from forze_socketio import build_socketio_server

sio = build_socketio_server(
    async_mode="asgi",
    cors_allowed_origins="*",
    redis_url="redis://localhost:6379/0",  # optional: multi-process backplane
)
```

Without `redis_url`, transport is in-process only (single worker).

## Command routing

Each command maps an event name to an operation key, validates the inbound payload, runs the handler through `ExecutionContext`, and optionally validates the ack.

```python
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext, OperationRegistry
from forze_socketio import (
    ForzeSocketIOAdapter,
    SocketIONamespaceRouter,
    make_registry_operation_resolver,
)


class JoinRoomCmd(BaseModel):
    room: str
    user_id: str


class JoinRoomAck(BaseModel):
    room: str
    joined: bool


class JoinRoom(Handler[JoinRoomCmd, JoinRoomAck]):
    async def __call__(self, args: JoinRoomCmd) -> JoinRoomAck:
        return JoinRoomAck(room=args.room, joined=True)


registry = OperationRegistry(handlers={"chat.join": lambda _ctx: JoinRoom()}).freeze()

router = SocketIONamespaceRouter(namespace="/chat").command(
    event="join",
    operation="chat.join",
    payload_type=JoinRoomCmd,
    ack_type=JoinRoomAck,
)


def context_factory(_request) -> ExecutionContext:
    return ExecutionContext(deps=Deps())


adapter = ForzeSocketIOAdapter(
    sio=sio,
    context_factory=context_factory,
    operation_resolver=make_registry_operation_resolver(registry),
)
adapter.include_router(router)
```

Flow: validate payload → `context_factory` → `operation_resolver(ctx, operation)` → `await handler(args)` → serialize ack.

### Multiple namespaces

```python
chat = SocketIONamespaceRouter(namespace="/chat").command(
    event="join",
    operation="chat.join",
    payload_type=JoinRoomCmd,
    ack_type=JoinRoomAck,
)
tasks = SocketIONamespaceRouter(namespace="/tasks").command(
    event="start",
    operation="tasks.start",
    payload_type=StartTaskCmd,
    ack_type=StartTaskAck,
)
adapter.include_routers(chat, tasks)
```

Bind `InvocationMetadata`, authn, and tenant identity in `context_factory` (or middleware around it) — handlers read `ctx.inv_ctx`, they do not bind at the socket layer themselves.

## Server events

Use `SocketIOEventEmitter` / `SocketIONamespaceEmitter` with `SocketIOServerEvent` for typed outbound payloads. See `forze_socketio.emitter`.

## ASGI

Mount with `build_socketio_asgi_app(sio, other_asgi_app=...)` when Socket.IO shares an app with FastAPI or another ASGI stack.

## Anti-patterns

1. **Unfrozen registry** — call `.freeze()` after binding transaction routes and stage hooks.
2. **Importing adapter code in handlers** — resolve ports from `ExecutionContext`.
3. **Skipping `context_factory` wiring** — every event needs a request-scoped context with the same `DepsPlan` as HTTP.
4. **Duplicate event names per namespace** — `SocketIONamespaceRouter.command` raises on collision.
