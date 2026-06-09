"""Structural protocol for GCS clients."""

from forze.application.integrations.storage.client import ObjectStorageClientPort

GCSClientPort = ObjectStorageClientPort

__all__ = ["GCSClientPort"]
