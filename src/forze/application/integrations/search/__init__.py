"""Shared search integration helpers."""

from .encryption import (
    decrypt_search_rows,
    reject_encrypted_sort_fields,
    resolve_search_read_codec_spec,
    resolve_snapshot_cipher,
    search_spec_encrypts,
)
from .multi_leg import (
    build_federated_highlight_index,
    federated_highlights_for_hits,
)
from .port import SimpleSearchPortMixin
from .snapshot import SearchResultSnapshot

__all__ = [
    "SearchResultSnapshot",
    "SimpleSearchPortMixin",
    "build_federated_highlight_index",
    "federated_highlights_for_hits",
    "decrypt_search_rows",
    "reject_encrypted_sort_fields",
    "resolve_search_read_codec_spec",
    "resolve_snapshot_cipher",
    "search_spec_encrypts",
]
