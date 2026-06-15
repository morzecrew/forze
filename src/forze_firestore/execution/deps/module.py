"""Firestore dependency module for the application kernel."""

from functools import partial
from typing import Any, Callable, cast, final

import attrs

from forze.application.contracts.crypto import EncryptionTier
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.document.wiring import derive_read_only_document_config
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
    warn_integration_routes,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import (
    merge_deps,
    routed_constant,
    routed_from_mapping,
)
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from ...kernel._logger import logger
from ...kernel.client import FirestoreClientPort, RoutedFirestoreClient
from ._warnings import FIRESTORE_DOCUMENT_RO_WARNING, FIRESTORE_DOCUMENT_RW_WARNING
from .configs import FirestoreDocumentConfig, FirestoreReadOnlyDocumentConfig
from .factories import (
    ConfigurableFirestoreDocument,
    ConfigurableFirestoreReadOnlyDocument,
    firestore_txmanager,
)
from .keys import FirestoreClientDepKey

# ----------------------- #


def _rw_document_query_factory(
    *,
    config: FirestoreDocumentConfig,
    required_encryption: EncryptionTier | None = None,
) -> ConfigurableFirestoreReadOnlyDocument[Any]:
    return ConfigurableFirestoreReadOnlyDocument(
        config=derive_read_only_document_config(
            config=config,  # type: ignore[arg-type]
            factory=FirestoreReadOnlyDocumentConfig,
        ),
        required_encryption=required_encryption,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FirestoreDepsModule(DepsModule):
    """Dependency module registering Firestore client, documents, and transactions."""

    client: FirestoreClientPort
    """Pre-constructed Firestore client."""

    ro_documents: StrKeyMapping[FirestoreReadOnlyDocumentConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from read-only document names to their Firestore-specific configurations."""

    rw_documents: StrKeyMapping[FirestoreDocumentConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from read-write document names to their Firestore-specific configurations."""

    tx: set[StrKey] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Documents span ``tagged`` (tenant filter via ``tenant_aware``) and ``dedicated`` (a routed
    per-tenant client).
    """

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum document field-encryption coverage (``None`` = no floor).

    When set, a document spec served by this module whose derived coverage is weaker
    is refused at resolution. Documents can only ever provide per-``field`` coverage.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="Firestore",
            routes=self.ro_documents,
            warning=FIRESTORE_DOCUMENT_RO_WARNING,
            log_warning=logger.warning,
        )
        warn_integration_routes(
            integration="Firestore",
            routes=self.rw_documents,
            warning=FIRESTORE_DOCUMENT_RW_WARNING,
            log_warning=logger.warning,
        )
        validate_module_tenancy(
            integration="Firestore",
            client_is_routed=isinstance(self.client, RoutedFirestoreClient),
            groups=[
                TenancyRouteGroup(
                    kind="document",
                    configs=self.ro_documents,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.read,
                ),
                TenancyRouteGroup(
                    kind="document",
                    configs=self.rw_documents,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.read,
                ),
            ],
            required_isolation=self.required_tenant_isolation,
            max_supported_isolation="dedicated",
            validation_failed_code="firestore_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        # ``cast`` erases the factories' generic parameters (``partial`` would otherwise
        # leak them as Unknown); ``routed_from_mapping`` only needs a plain callable.
        ro_query_factory = cast(
            Callable[..., Any],
            partial(
                ConfigurableFirestoreReadOnlyDocument,
                required_encryption=self.required_encryption,
            ),
        )
        rw_query_factory = cast(
            Callable[..., Any],
            partial(
                _rw_document_query_factory,
                required_encryption=self.required_encryption,
            ),
        )
        rw_command_factory = cast(
            Callable[..., Any],
            partial(
                ConfigurableFirestoreDocument,
                required_encryption=self.required_encryption,
            ),
        )

        return merge_deps(
            routed_from_mapping(
                self.ro_documents,
                bindings=[(DocumentQueryDepKey, ro_query_factory)],
            ),
            routed_from_mapping(
                self.rw_documents,
                bindings=[
                    (DocumentQueryDepKey, rw_query_factory),
                    (DocumentCommandDepKey, rw_command_factory),
                ],
            ),
            routed_constant(
                self.tx,
                bindings=[(TransactionManagerDepKey, firestore_txmanager)],
            ),
            plain={FirestoreClientDepKey: self.client},
        )
