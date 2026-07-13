"""Mongo dependency module for the application kernel."""

from collections.abc import Callable
from functools import partial
from typing import Any, cast, final

import attrs

from forze.application.contracts.crypto import EncryptionTier
from forze.application.contracts.deps import (
    Deps,
    DepsModule,
    merge_deps,
    routed_constant,
    routed_from_mapping,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.document.wiring import derive_read_only_document_config
from forze.application.contracts.outbox import OutboxCommandDepKey, OutboxQueryDepKey
from forze.application.contracts.search import SearchQueryDepKey
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
    warn_integration_routes,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from ...kernel._logger import logger
from ...kernel.client import MongoClientPort, RoutedMongoClient
from ._warnings import (
    MONGO_DOCUMENT_RO_WARNING,
    MONGO_DOCUMENT_RW_WARNING,
    MONGO_OUTBOX_WARNING,
    MONGO_SEARCH_WARNING,
)
from .configs import (
    MongoDocumentConfig,
    MongoOutboxConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
)
from .factories import (
    ConfigurableMongoDocument,
    ConfigurableMongoOutboxCommand,
    ConfigurableMongoOutboxQuery,
    ConfigurableMongoReadOnlyDocument,
    ConfigurableMongoSearch,
    mongo_txmanager,
)
from .keys import MongoClientDepKey

# ----------------------- #


def _rw_document_query_factory(
    *,
    config: MongoDocumentConfig,
    required_encryption: EncryptionTier | None = None,
) -> ConfigurableMongoReadOnlyDocument[Any]:
    return ConfigurableMongoReadOnlyDocument(
        config=derive_read_only_document_config(
            config=config,  # type: ignore[arg-type]
            factory=MongoReadOnlyDocumentConfig,
        ),
        required_encryption=required_encryption,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDepsModule(DepsModule):
    """Dependency module that registers Mongo client, tx manager, and document port."""

    client: MongoClientPort
    """Pre-constructed Mongo client (single-URI or routed)."""

    ro_documents: StrKeyMapping[MongoReadOnlyDocumentConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from read-only document names to their Mongo-specific configurations."""

    rw_documents: StrKeyMapping[MongoDocumentConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from read-write document names to their Mongo-specific configurations."""

    tx: set[StrKey] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    searches: StrKeyMapping[MongoSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from search spec names to Mongo-specific search configurations."""

    outboxes: StrKeyMapping[MongoOutboxConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from outbox route names to Mongo-specific configurations."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Documents/search/outbox span ``tagged`` (tenant filter via ``tenant_aware``) and
    ``dedicated`` (a routed per-tenant client).
    """

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum document field-encryption coverage (``None`` = no floor).

    When set, a document spec served by this module whose derived coverage is weaker
    is refused at resolution. Documents can only ever provide per-``field`` coverage.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="Mongo",
            routes=self.ro_documents,
            warning=MONGO_DOCUMENT_RO_WARNING,
            log_warning=logger.warning,
        )
        warn_integration_routes(
            integration="Mongo",
            routes=self.rw_documents,
            warning=MONGO_DOCUMENT_RW_WARNING,
            log_warning=logger.warning,
        )
        warn_integration_routes(
            integration="Mongo",
            routes=self.searches,
            warning=MONGO_SEARCH_WARNING,
            log_warning=logger.warning,
        )
        warn_integration_routes(
            integration="Mongo",
            routes=self.outboxes,
            warning=MONGO_OUTBOX_WARNING,
            log_warning=logger.warning,
        )
        validate_module_tenancy(
            integration="Mongo",
            client_is_routed=isinstance(self.client, RoutedMongoClient),
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
                TenancyRouteGroup(
                    kind="search",
                    configs=self.searches,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.index_name,
                ),
                TenancyRouteGroup(
                    kind="outbox",
                    configs=self.outboxes,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                ),
            ],
            required_isolation=self.required_tenant_isolation,
            max_supported_isolation="dedicated",
            validation_failed_code="mongo_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Mongo-backed ports."""

        # ``cast`` erases the factories' generic parameters (``partial`` would otherwise
        # leak them as Unknown); ``routed_from_mapping`` only needs a plain callable.
        ro_query_factory = cast(
            Callable[..., Any],
            partial(
                ConfigurableMongoReadOnlyDocument,
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
                ConfigurableMongoDocument,
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
            routed_from_mapping(
                self.searches,
                bindings=[(SearchQueryDepKey, ConfigurableMongoSearch)],
            ),
            routed_constant(
                self.tx,
                bindings=[(TransactionManagerDepKey, mongo_txmanager)],
            ),
            routed_from_mapping(
                self.outboxes,
                bindings=[
                    (OutboxCommandDepKey, ConfigurableMongoOutboxCommand),
                    (OutboxQueryDepKey, ConfigurableMongoOutboxQuery),
                ],
            ),
            plain={MongoClientDepKey: self.client},
        )
