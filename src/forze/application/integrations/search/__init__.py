"""Shared search integration helpers."""

from ._snapshot_stream import (
    SnapshotStreamResult,
    SnapshotWindow,
    build_snapshot_pool_streaming,
)
from .encryption import (
    decrypt_search_rows,
    reject_encrypted_sort_fields,
    resolve_search_read_codec_spec,
    resolve_snapshot_cipher,
    search_spec_encrypts,
)
from .federated_executor import (
    execute_federated_thin_offset,
    federated_snapshot_rehydrator,
    federated_thin_eligible,
    federated_thin_format,
)
from .multi_leg import (
    build_federated_highlight_index,
    federated_highlights_for_hits,
)
from .port import SimpleSearchPortMixin
from .snapshot import SearchResultSnapshot
from .stream import DEFAULT_MAX_SEARCH_STREAM_PAGES, stream_search_pages

__all__ = [
    "DEFAULT_MAX_SEARCH_STREAM_PAGES",
    "SearchResultSnapshot",
    "SimpleSearchPortMixin",
    "stream_search_pages",
    "SnapshotStreamResult",
    "SnapshotWindow",
    "build_snapshot_pool_streaming",
    "build_federated_highlight_index",
    "execute_federated_thin_offset",
    "federated_snapshot_rehydrator",
    "federated_thin_eligible",
    "federated_thin_format",
    "federated_highlights_for_hits",
    "decrypt_search_rows",
    "reject_encrypted_sort_fields",
    "resolve_search_read_codec_spec",
    "resolve_snapshot_cipher",
    "search_spec_encrypts",
]
