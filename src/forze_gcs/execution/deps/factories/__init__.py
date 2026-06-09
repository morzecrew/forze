"""GCS dependency factories."""

from .storage import ConfigurableGCSStorageCommand, ConfigurableGCSStorageQuery

# ----------------------- #

__all__ = ["ConfigurableGCSStorageQuery", "ConfigurableGCSStorageCommand"]
