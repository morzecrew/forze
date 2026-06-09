"""Firestore dependency module for the application kernel."""

from typing import Any, Mapping, final

import attrs

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.document.wiring import derive_read_only_document_config
from forze.application.contracts.tenancy import warn_integration_routes
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import (
    merge_deps,
    routed_constant,
    routed_from_mapping,
)
from forze.base.primitives import StrKey

from ...kernel._logger import logger
from ...kernel.client import FirestoreClientPort
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
) -> ConfigurableFirestoreReadOnlyDocument[Any]:
    return ConfigurableFirestoreReadOnlyDocument(
        config=derive_read_only_document_config(
            config=config,  # type: ignore[arg-type]
            factory=FirestoreReadOnlyDocumentConfig,
        ),
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FirestoreDepsModule(DepsModule):
    """Dependency module registering Firestore client, documents, and transactions."""

    client: FirestoreClientPort
    ro_documents: Mapping[StrKey, FirestoreReadOnlyDocumentConfig] | None = attrs.field(
        default=None
    )
    rw_documents: Mapping[StrKey, FirestoreDocumentConfig] | None = attrs.field(
        default=None
    )
    tx: set[StrKey] | None = attrs.field(default=None)

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

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.ro_documents,
                bindings=[(DocumentQueryDepKey, ConfigurableFirestoreReadOnlyDocument)],
            ),
            routed_from_mapping(
                self.rw_documents,
                bindings=[
                    (DocumentQueryDepKey, _rw_document_query_factory),
                    (DocumentCommandDepKey, ConfigurableFirestoreDocument),
                ],
            ),
            routed_constant(
                self.tx,
                bindings=[(TransactionManagerDepKey, firestore_txmanager)],
            ),
            plain={FirestoreClientDepKey: self.client},
        )
