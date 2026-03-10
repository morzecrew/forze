# Socket.IO Integration

`forze_socketio` connects Forze usecases to real-time Socket.IO events. It provides typed command routing, usecase dispatch through `ExecutionContext`, typed server event emission, and optional Redis backplane support for distributed deployments.

## Installation

```bash
uv add 'forze[socketio]'
```

## Building a server

Create an async Socket.IO server with `build_socketio_server`. Optionally enable the Redis manager for multi-process clustering:

```python
from forze_socketio import build_socketio_server

sio = build_socketio_server(
    async_mode="asgi",
    cors_allowed_origins="*",
    redis_url="redis://localhost:6379/0",  # optional, for distributed setups
)
```

Without `redis_url`, the server uses in-memory transport (single-process only).

## Command event routing

Define namespace routes as typed command events. Each event validates input against a Pydantic model, builds an `ExecutionContext`, resolves the usecase, executes it, and returns a validated acknowledgement payload.

### Define DTOs

```python
from pydantic import BaseModel


class JoinRoomCmd(BaseModel):
    room: str
    user_id: str


class JoinRoomAck(BaseModel):
    room: str
    joined: bool
```

### Create a usecase

```python
from forze.application.execution import Usecase


class JoinRoom(Usecase[JoinRoomCmd, JoinRoomAck]):
    async def main(self, args: JoinRoomCmd) -> JoinRoomAck:
        # Business logic here
        return JoinRoomAck(room=args.room, joined=True)
```

### Register and route

```python
from forze.application.execution import Deps, ExecutionContext, UsecaseRegistry
from forze_socketio import (
    ForzeSocketIOAdapter,
    SocketIONamespaceRouter,
    make_registry_usecase_resolver,
)

registry = UsecaseRegistry().register(
    "chat.join",
    lambda ctx: JoinRoom(ctx=ctx),
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
    usecase_resolver=make_registry_usecase_resolver(registry),
)
adapter.include_router(router)
```

The adapter handles:

1. Deserializing the incoming event payload into `JoinRoomCmd`
2. Calling `context_factory` to create a request-scoped `ExecutionContext`
3. Resolving `chat.join` from the registry with the context
4. Executing the usecase and serializing the result as `JoinRoomAck`
5. Returning the acknowledgement to the client

### Multiple namespaces

Register multiple routers for different namespaces:

```python
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
```

## Server event emission

Use `SocketIOEventEmitter` to send typed server events. The emitter validates payloads against Pydantic models before sending, ensuring type safety for outbound events.

### Define a server event

```python
from forze_socketio import SocketIOEventEmitter, SocketIOServerEvent


class ProgressPayload(BaseModel):
    done: int
    total: int


progress_event = SocketIOServerEvent(
    event="task.progress",
    namespace="/tasks",
    payload_type=ProgressPayload,
)
```

### Emit

```python
emitter = SocketIOEventEmitter(sio=sio)

await emitter.emit(
    progress_event,
    ProgressPayload(done=3, total=10),
    room="task:42",
)
```

The emitter can target specific rooms, sessions (SIDs), or broadcast to all connected clients.

## ASGI integration

Wrap the Socket.IO server as an ASGI application. This allows running alongside FastAPI or other ASGI frameworks:

```python
from forze_socketio import build_socketio_asgi_app

app = build_socketio_asgi_app(sio, other_asgi_app=fastapi_app)
```

### Combined FastAPI + Socket.IO

```python
import uvicorn
from fastapi import FastAPI
from forze_socketio import build_socketio_server, build_socketio_asgi_app

fastapi_app = FastAPI(title="My App")
sio = build_socketio_server(async_mode="asgi")

# Set up Socket.IO routing (as shown above)
# ...

# Combine into a single ASGI app
app = build_socketio_asgi_app(sio, other_asgi_app=fastapi_app)

uvicorn.run(app, host="0.0.0.0", port=8000)
```

HTTP requests go to FastAPI. WebSocket connections for Socket.IO are handled by the `sio` server. Both share the same ASGI process.

## Context factory

The context factory creates a request-scoped `ExecutionContext` for each incoming Socket.IO event. In production, you typically wire it to the same runtime used by FastAPI:

```python
def context_factory(request) -> ExecutionContext:
    return runtime.get_context()
```

The `request` parameter carries Socket.IO metadata (SID, namespace, event name) that you can use for tenant/actor resolution or logging.

## Usecase resolver

The default `make_registry_usecase_resolver(registry)` looks up usecases by operation key in a `UsecaseRegistry`. For custom resolution logic (e.g. dynamic registration, permission checks), implement your own resolver:

```python
def custom_resolver(ctx: ExecutionContext, operation: str):
    if operation.startswith("admin."):
        check_admin_permissions(ctx)
    return registry.resolve(operation, ctx)


adapter = ForzeSocketIOAdapter(
    sio=sio,
    context_factory=context_factory,
    usecase_resolver=custom_resolver,
)
```
