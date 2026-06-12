"""GCS dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
)
from forze.application.contracts.tenancy import warn_integration_routes
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel._logger import logger
from ...kernel.client import GCSClientPort
from ._warnings import GCS_STORAGE_WARNING
from .configs import GCSStorageConfig
from .factories import ConfigurableGCSStorageCommand, ConfigurableGCSStorageQuery
from .keys import GCSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GCSDepsModule(DepsModule):
    """Dependency module that registers GCS client and storage port."""

    client: GCSClientPort
    """Pre-constructed GCS client (initialized via :func:`gcs_lifecycle_step`)."""

    storages: StrKeyMapping[GCSStorageConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
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
                bindings=[
                    (StorageQueryDepKey, ConfigurableGCSStorageQuery),
                    (StorageCommandDepKey, ConfigurableGCSStorageCommand),
                ],
            ),
            plain={GCSClientDepKey: self.client},
        )
