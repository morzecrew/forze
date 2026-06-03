"""Event type constants for stored-file integration events."""

from typing import final

# ----------------------- #


@final
class StoredFileEventType:
    """Integration event type strings for stored-file outbox staging."""

    UPLOAD_PENDING = "stored_file.upload_pending"
    """A stored-file row was created with ``pending`` status before blob upload."""

    UPLOADED = "stored_file.uploaded"
    """Blob upload completed and the stored-file row is ``ready``."""

    DELETED = "stored_file.deleted"
    """A stored-file row was soft-deleted."""
