"""GCS dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.integrations.storage import ObjectStorageDepsModule

from ...kernel._logger import logger
from ...kernel.client import GCSClientPort, RoutedGCSClient
from ._warnings import GCS_STORAGE_WARNING
from .configs import GCSStorageConfig
from .factories import (
    ConfigurableGCSStorageCommand,
    ConfigurableGCSStorageQuery,
    ConfigurableGCSStorageUploads,
)
from .keys import GCSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSDepsModule(ObjectStorageDepsModule[GCSClientPort, GCSStorageConfig]):
    """Dependency module that registers the GCS client and storage ports.

    The client must be initialized separately (e.g. via :func:`gcs_lifecycle_step`)
    before usecases run. Shared wiring/validation lives in
    :class:`~forze.application.integrations.storage.ObjectStorageDepsModule`.
    """

    integration_label = "GCS"
    client_dep_key = GCSClientDepKey
    routed_client_type = RoutedGCSClient
    route_warning = GCS_STORAGE_WARNING
    log_warning = logger.warning
    storage_query_factory = ConfigurableGCSStorageQuery
    storage_command_factory = ConfigurableGCSStorageCommand
    storage_uploads_factory = ConfigurableGCSStorageUploads
