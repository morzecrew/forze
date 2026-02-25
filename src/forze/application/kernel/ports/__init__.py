"""Aggregate exports for kernel ports.

This module re-exports the main port protocols that define how the application
kernel talks to infrastructure and external services.
"""

from .counter import CounterPort
from .document import (
    DocumentCachePort,
    DocumentPort,
    DocumentReadPort,
    DocumentSearchOptions,
    DocumentSearchPort,
    DocumentSorts,
    DocumentWritePort,
)
from .idempotency import IdempotencyPort, IdempotencySnapshot
from .runtime import AppRuntimePort
from .storage import DownloadedObject, ObjectMetadata, StoragePort, StoredObject
from .stream import StreamEvent, StreamPort
from .txmanager import TxManagerPort
from .workflow import WorkflowPort

# ----------------------- #

__all__ = [
    "WorkflowPort",
    "DocumentPort",
    "DocumentReadPort",
    "DocumentSearchPort",
    "DocumentWritePort",
    "DocumentSearchOptions",
    "DocumentSorts",
    "AppRuntimePort",
    "CounterPort",
    "DocumentCachePort",
    "StoragePort",
    "StoredObject",
    "ObjectMetadata",
    "DownloadedObject",
    "IdempotencyPort",
    "IdempotencySnapshot",
    "StreamPort",
    "StreamEvent",
    "TxManagerPort",
]
