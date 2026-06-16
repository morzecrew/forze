"""GCS dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageUploadSessionDepKey,
)
from forze.application.contracts.crypto import EncryptionTier
from forze.application.contracts.tenancy import (
    TenantIsolationMode,
    warn_integration_routes,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.application.integrations.storage import (
    validate_storage_encryption_wiring,
    validate_storage_tenancy_wiring,
)
from forze.base.primitives import MappingConverter, StrKeyMapping

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
class GCSDepsModule(DepsModule):
    """Dependency module that registers GCS client and storage port."""

    client: GCSClientPort
    """Pre-constructed GCS client (initialized via :func:`gcs_lifecycle_step`)."""

    storages: StrKeyMapping[GCSStorageConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from storage route names to GCS bucket configuration."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Object storage spans the full ladder: ``tagged`` (per-tenant path prefix via
    ``tenant_aware``), ``namespace`` (a per-tenant ``bucket`` resolver), ``dedicated`` (a
    routed per-tenant client / credentials). Wiring fails closed if the derived tier is
    weaker than the declared floor.
    """

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum encryption coverage (``None`` = no floor).

    Object storage does whole-object ``envelope`` encryption when a route sets
    ``encrypt=True``; wiring fails closed if a route's coverage is weaker than the
    declared floor. Requires a ``KeyringDepKey`` (e.g. via ``CryptoDepsModule``)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="GCS",
            routes=self.storages,
            warning=GCS_STORAGE_WARNING,
            log_warning=logger.warning,
        )
        validate_storage_tenancy_wiring(
            integration="GCS",
            client_is_routed=isinstance(self.client, RoutedGCSClient),
            storages=self.storages,
            required_isolation=self.required_tenant_isolation,
            validation_failed_code="gcs_storage_tenancy_validation_failed",
            log_warning=logger.warning,
        )
        validate_storage_encryption_wiring(
            integration="GCS",
            storages=self.storages,
            required_encryption=self.required_encryption,
            validation_failed_code="gcs_storage_encryption_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.storages,
                bindings=[
                    (StorageQueryDepKey, ConfigurableGCSStorageQuery),
                    (StorageCommandDepKey, ConfigurableGCSStorageCommand),
                    (StorageUploadSessionDepKey, ConfigurableGCSStorageUploads),
                ],
            ),
            plain={GCSClientDepKey: self.client},
        )
