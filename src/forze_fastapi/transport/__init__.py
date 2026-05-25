"""Shared transport helpers and composition presets for exposing Forze operations.

HTTP route attach lives in :mod:`forze_fastapi.transport.http` (``attach_*_routes``,
``register_route``, policies, ETag, idempotency). Real-time Socket.IO command routing
uses the separate :mod:`forze_socketio` package — not under ``transport.http``.
"""

from forze.application.composition.document import DocumentPreset
from forze.application.composition.search import SearchPreset
from forze.application.composition.storage import StoragePreset

# Attach APIs: import from forze_fastapi.transport.http

__all__ = [
    "DocumentPreset",
    "SearchPreset",
    "StoragePreset",
]
