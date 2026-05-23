"""Socket.IO transport integration for Forze."""

from ._compat import require_socketio

require_socketio()

# ....................... #

from forze.application.execution import make_registry_operation_resolver

from .emitter import (
    SocketIOEventEmitter,
    SocketIONamespaceEmitter,
    SocketIOServerEvent,
)
from .routing import (
    ExecutionContextFactoryPort,
    ForzeSocketIOAdapter,
    HandlerResolverPort,
    SocketIOCommandRoute,
    SocketIONamespaceRouter,
    SocketIORequest,
)
from .server import build_socketio_asgi_app, build_socketio_server

# ----------------------- #

__all__ = [
    "ExecutionContextFactoryPort",
    "HandlerResolverPort",
    "SocketIORequest",
    "SocketIOCommandRoute",
    "SocketIONamespaceRouter",
    "ForzeSocketIOAdapter",
    "make_registry_operation_resolver",
    "SocketIOServerEvent",
    "SocketIOEventEmitter",
    "SocketIONamespaceEmitter",
    "build_socketio_server",
    "build_socketio_asgi_app",
]
