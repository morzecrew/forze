"""Structural protocol for S3 clients (single endpoint or tenant-routed)."""

from forze.application.integrations.storage.client import ObjectStorageClientPort

S3ClientPort = ObjectStorageClientPort

__all__ = ["S3ClientPort"]
