"""GCS dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.storage import StorageDepKey
from forze.application.contracts.tenancy import warn_integration_routes
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import StrKey

from ...kernel._logger import logger
from ...kernel.client import GCSClientPort
from ._warnings import GCS_STORAGE_WARNING
from .configs import GCSStorageConfig
from .factories import ConfigurableGCSStorage
from .keys import GCSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSDepsModule(DepsModule):
    """Dependency module that registers GCS client and storage port."""

    client: GCSClientPort
    """Pre-constructed GCS client (initialized via :func:`gcs_lifecycle_step`)."""

    storages: Mapping[StrKey, GCSStorageConfig] | None = attrs.field(default=None)
    """Mapping from storage route names to GCS bucket configuration."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="GCS",
            routes=self.storages,
            warning=GCS_STORAGE_WARNING,
            log_warning=logger.warning,
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.storages,
                bindings=[(StorageDepKey, ConfigurableGCSStorage)],
            ),
            plain={GCSClientDepKey: self.client},
        )
