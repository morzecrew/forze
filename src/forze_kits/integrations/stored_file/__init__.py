"""Backward-compatible re-exports for stored-file wiring (canonical: ``forze_kits.aggregates.stored_file``)."""

from forze_kits.aggregates.stored_file import (
    StoredFileOutboxPayload,
    bind_stored_file_writes,
    freeze_stored_file_registry,
    stored_file_complete_upload_after_commit_factory,
    stored_file_outbox_flush_factory,
    stored_file_purge_blob_after_commit_factory,
)

# ----------------------- #

__all__ = [
    "StoredFileOutboxPayload",
    "bind_stored_file_writes",
    "freeze_stored_file_registry",
    "stored_file_complete_upload_after_commit_factory",
    "stored_file_outbox_flush_factory",
    "stored_file_purge_blob_after_commit_factory",
]
