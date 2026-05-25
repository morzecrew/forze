"""Dependency keys for GCS-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import GCSClientPort

# ----------------------- #

GCSClientDepKey: DepKey[GCSClientPort] = DepKey("gcs_client")
"""Key used to register a GCS client in the deps container."""
