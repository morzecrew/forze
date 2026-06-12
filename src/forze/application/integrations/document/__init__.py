"""Shared document adapter base and cache helpers."""

from .adapter import DocumentAdapter
from .cache import DocumentCache
from .l1 import L1Stats, L1Store, LruTtlStore

__all__ = ["DocumentAdapter", "DocumentCache", "L1Stats", "L1Store", "LruTtlStore"]
