"""S3 dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
)
from forze.application.contracts.tenancy import (
    TenantIsolationMode,
    warn_integration_routes,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.application.integrations.storage import validate_storage_tenancy_wiring
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel._logger import logger
from ...kernel.client import RoutedS3Client, S3ClientPort
from ._warnings import S3_STORAGE_WARNING
from .configs import S3StorageConfig
from .factories import ConfigurableS3StorageCommand, ConfigurableS3StorageQuery
from .keys import S3ClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class S3DepsModule(DepsModule):
    """Dependency module that registers S3 client and storage port.

    Invoke to produce a :class:`Deps` container with S3-backed storage
    dependencies. The client must be initialized separately (e.g. via
    :func:`s3_lifecycle_step`) before usecases run.
    """

    client: S3ClientPort
    """Pre-constructed S3 client (single endpoint or routed, session not initialized until lifecycle)."""

    storages: StrKeyMapping[S3StorageConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from storage names to their S3-specific configurations."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Object storage spans the full ladder: ``tagged`` (per-tenant path prefix via
    ``tenant_aware``), ``namespace`` (a per-tenant ``bucket`` resolver), ``dedicated`` (a
    routed per-tenant client / credentials). Wiring fails closed if the derived tier is
    weaker than the declared floor.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="S3",
            routes=self.storages,
            warning=S3_STORAGE_WARNING,
            log_warning=logger.warning,
        )
        validate_storage_tenancy_wiring(
            integration="S3",
            client_is_routed=isinstance(self.client, RoutedS3Client),
            storages=self.storages,
            required_isolation=self.required_tenant_isolation,
            validation_failed_code="s3_storage_tenancy_validation_failed",
            log_warning=logger.warning,
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with S3-backed storage port.

        :returns: Deps with client and storage port factory.
        """

        return merge_deps(
            routed_from_mapping(
                self.storages,
                bindings=[
                    (StorageQueryDepKey, ConfigurableS3StorageQuery),
                    (StorageCommandDepKey, ConfigurableS3StorageCommand),
                ],
            ),
            plain={S3ClientDepKey: self.client},
        )
