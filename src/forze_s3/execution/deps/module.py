"""S3 dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.integrations.storage import ObjectStorageDepsModule

from ...kernel._logger import logger
from ...kernel.client import RoutedS3Client, S3ClientPort
from ._warnings import S3_STORAGE_WARNING
from .configs import S3StorageConfig
from .factories import (
    ConfigurableS3StorageCommand,
    ConfigurableS3StorageQuery,
    ConfigurableS3StorageUploads,
)
from .keys import S3ClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3DepsModule(ObjectStorageDepsModule[S3ClientPort, S3StorageConfig]):
    """Dependency module that registers the S3 client and storage ports.

    Invoke to produce a :class:`Deps` container with S3-backed storage dependencies. The
    client must be initialized separately (e.g. via :func:`s3_lifecycle_step`) before
    usecases run. Shared wiring/validation lives in
    :class:`~forze.application.integrations.storage.ObjectStorageDepsModule`.
    """

    integration_label = "S3"
    client_dep_key = S3ClientDepKey
    routed_client_type = RoutedS3Client
    route_warning = S3_STORAGE_WARNING
    log_warning = logger.warning
    storage_query_factory = ConfigurableS3StorageQuery
    storage_command_factory = ConfigurableS3StorageCommand
    storage_uploads_factory = ConfigurableS3StorageUploads
