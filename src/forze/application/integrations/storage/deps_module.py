"""Shared object-storage ``DepsModule`` base for S3/GCS-style backends."""

from collections.abc import Callable
from typing import Any, ClassVar, Generic, TypeVar

import attrs

from forze.application.contracts.crypto import EncryptionTier
from forze.application.contracts.deps import (
    DepKey,
    Deps,
    DepsModule,
    merge_deps,
    routed_from_mapping,
)
from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageUploadSessionDepKey,
)
from forze.application.contracts.tenancy import (
    TenantIsolationMode,
    warn_integration_routes,
)
from forze.base.primitives import MappingConverter, StrKeyMapping

from .encryption import validate_storage_encryption_wiring
from .tenancy import validate_storage_tenancy_wiring

# ----------------------- #

ClientT = TypeVar("ClientT")
ConfigT = TypeVar("ConfigT")


@attrs.define(slots=True, frozen=True, kw_only=True)
class ObjectStorageDepsModule(DepsModule, Generic[ClientT, ConfigT]):
    """Shared ``DepsModule`` for object-storage backends (S3, GCS, ...).

    Concrete backends subclass this, parameterize it with their client/config types, and
    set the class-level hooks (label, client dep key, routed-client type, route warning,
    logger, and the configurable Query/Command/Uploads factories). The identical route
    warning, tenancy/encryption wiring validation, and ``Deps`` assembly live here once.
    """

    client: ClientT
    """Pre-constructed client (single endpoint or routed; session initialized via the
    backend's lifecycle step before usecases run)."""

    storages: StrKeyMapping[ConfigT] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from storage route names to backend-specific configuration."""

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

    # --- per-backend hooks (set as class attributes on each subclass) --- #

    integration_label: ClassVar[str]
    """Backend label, e.g. ``"S3"`` / ``"GCS"``; also derives the validation codes."""

    client_dep_key: ClassVar[DepKey[Any]]
    """Plain dep key the client is registered under."""

    routed_client_type: ClassVar[type]
    """Routed-client class, used for the derived per-tenant isolation check."""

    route_warning: ClassVar[Any]
    """``IntegrationRouteWarning`` emitted when routes are configured (see
    ``warn_integration_routes``)."""

    log_warning: ClassVar[Callable[..., None]]
    """The backend logger's ``warning`` callable."""

    storage_query_factory: ClassVar[Any]
    """Configurable ``StorageQueryPort`` factory for this backend."""

    storage_command_factory: ClassVar[Any]
    """Configurable ``StorageCommandPort`` factory for this backend."""

    storage_uploads_factory: ClassVar[Any]
    """Configurable ``StorageUploadSessionPort`` factory for this backend."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        label = self.integration_label
        # The concrete subclass binds ConfigT to a real storage-route config that
        # satisfies the validators' structural protocols; the generic erases that, so
        # bridge with a single Any-typed view rather than bounding ConfigT to a private
        # protocol.
        routes: Any = self.storages

        warn_integration_routes(
            integration=label,
            routes=routes,
            warning=self.route_warning,
            log_warning=self.log_warning,
        )
        validate_storage_tenancy_wiring(
            integration=label,
            client_is_routed=isinstance(self.client, self.routed_client_type),
            storages=routes,
            required_isolation=self.required_tenant_isolation,
            validation_failed_code=f"{label.lower()}_storage_tenancy_validation_failed",
            log_warning=self.log_warning,
        )
        validate_storage_encryption_wiring(
            integration=label,
            storages=routes,
            required_encryption=self.required_encryption,
            validation_failed_code=f"{label.lower()}_storage_encryption_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.storages,
                bindings=[
                    (StorageQueryDepKey, self.storage_query_factory),
                    (StorageCommandDepKey, self.storage_command_factory),
                    (StorageUploadSessionDepKey, self.storage_uploads_factory),
                ],
            ),
            plain={self.client_dep_key: self.client},
        )
