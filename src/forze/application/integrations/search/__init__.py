"""Shared search integration helpers."""

from .encryption import (
    decrypt_search_rows,
    reject_encrypted_sort_fields,
    resolve_search_read_codec_spec,
    resolve_snapshot_cipher,
    search_spec_encrypts,
)
from .port import SimpleSearchPortMixin
from .snapshot import SearchResultSnapshot

__all__ = [
    "SearchResultSnapshot",
    "SimpleSearchPortMixin",
    "decrypt_search_rows",
    "reject_encrypted_sort_fields",
    "resolve_search_read_codec_spec",
    "resolve_snapshot_cipher",
    "search_spec_encrypts",
]
