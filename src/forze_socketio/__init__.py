"""Socket.IO transport integration for Forze."""

from ._compat import require_socketio

require_socketio()

# ....................... #

from .emitter import (
    SocketIOEventEmitter,
    SocketIONamespaceEmitter,
    SocketIOServerEvent,
)
from .routing import (
    ExecutionContextFactoryPort,
    ForzeSocketIOAdapter,
    SocketIOCommandRoute,
    SocketIONamespaceRouter,
    SocketIORequest,
    UsecaseResolverPort,
    make_registry_usecase_resolver,
)
from .server import build_socketio_asgi_app, build_socketio_server

# ----------------------- #

__all__ = [
    "ExecutionContextFactoryPort",
    "UsecaseResolverPort",
    "SocketIORequest",
    "SocketIOCommandRoute",
    "SocketIONamespaceRouter",
    "ForzeSocketIOAdapter",
    "make_registry_usecase_resolver",
    "SocketIOServerEvent",
    "SocketIOEventEmitter",
    "SocketIONamespaceEmitter",
    "build_socketio_server",
    "build_socketio_asgi_app",
]
