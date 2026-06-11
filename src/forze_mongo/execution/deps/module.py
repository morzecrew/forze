"""Mongo dependency module for the application kernel."""

from typing import Any, final

import attrs

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.document.wiring import derive_read_only_document_config
from forze.application.contracts.outbox import OutboxCommandDepKey, OutboxQueryDepKey
from forze.application.contracts.search import SearchQueryDepKey
from forze.application.contracts.tenancy import warn_integration_routes
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import (
    merge_deps,
    routed_constant,
    routed_from_mapping,
)
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from ...kernel._logger import logger
from ...kernel.client import MongoClientPort
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
) -> ConfigurableMongoReadOnlyDocument[Any]:
    return ConfigurableMongoReadOnlyDocument(
        config=derive_read_only_document_config(
            config=config,  # type: ignore[arg-type]
            factory=MongoReadOnlyDocumentConfig,
        ),
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
        converter=MappingConverter.to_str_key_frozen, # type: ignore[misc]
    )
    """Mapping from read-only document names to their Mongo-specific configurations."""

    rw_documents: StrKeyMapping[MongoDocumentConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen, # type: ignore[misc]
    )
    """Mapping from read-write document names to their Mongo-specific configurations."""

    tx: set[StrKey] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    searches: StrKeyMapping[MongoSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen, # type: ignore[misc]
    )
    """Mapping from search spec names to Mongo-specific search configurations."""

    outboxes: StrKeyMapping[MongoOutboxConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen, # type: ignore[misc]
    )
    """Mapping from outbox route names to Mongo-specific configurations."""

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

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Mongo-backed ports."""

        return merge_deps(
            routed_from_mapping(
                self.ro_documents,
                bindings=[(DocumentQueryDepKey, ConfigurableMongoReadOnlyDocument)],
            ),
            routed_from_mapping(
                self.rw_documents,
                bindings=[
                    (DocumentQueryDepKey, _rw_document_query_factory),
                    (DocumentCommandDepKey, ConfigurableMongoDocument),
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
