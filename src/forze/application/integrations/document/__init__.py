"""Shared document adapter base and cache helpers."""

from .adapter import DocumentAdapter
from .cache import DocumentCache
from .l1 import L1Stats, L1Store, LruTtlStore, TinyLfuStore, tiny_lfu_l1_store
from .observability import instrument_document_l1

__all__ = [
    "DocumentAdapter",
    "DocumentCache",
    "L1Stats",
    "L1Store",
    "LruTtlStore",
    "TinyLfuStore",
    "instrument_document_l1",
    "tiny_lfu_l1_store",
]
