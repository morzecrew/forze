"""Firestore dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.tenancy import warn_dynamic_relation_with_tenant_aware
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import StrKey

from ...kernel._logger import logger
from ...kernel.platform import FirestoreClientPort
from .configs import FirestoreDocumentConfig, FirestoreReadOnlyDocumentConfig
from .deps import (
    ConfigurableFirestoreDocument,
    ConfigurableFirestoreReadOnlyDocument,
    firestore_txmanager,
)
from .keys import FirestoreClientDepKey

# ----------------------- #


def _document_config_to_read_only(
    config: FirestoreDocumentConfig,
) -> FirestoreReadOnlyDocumentConfig:
    return FirestoreReadOnlyDocumentConfig(
        read=config.read,
        tenant_aware=config.tenant_aware,
        batch_size=config.batch_size,
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
        if self.ro_documents:
            for name, cfg in self.ro_documents.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="Firestore",
                    route_name=str(name),
                    kind="document",
                    tenant_aware=cfg.tenant_aware,
                    relation_fields=[("read", cfg.read)],
                    log_warning=logger.warning,
                )

        if self.rw_documents:
            for name, cfg in self.rw_documents.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="Firestore",
                    route_name=str(name),
                    kind="document",
                    tenant_aware=cfg.tenant_aware,
                    relation_fields=[
                        ("read", cfg.read),
                        ("write", cfg.write),
                        ("history", cfg.history),
                    ],
                    log_warning=logger.warning,
                )

    # ....................... #

    def __call__(self) -> Deps:
        plain_deps = Deps.plain({FirestoreClientDepKey: self.client})
        doc_deps = Deps()
        tx_deps = Deps()

        if self.ro_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentQueryDepKey: {
                            name: ConfigurableFirestoreReadOnlyDocument(config=config)
                            for name, config in self.ro_documents.items()
                        }
                    }
                )
            )

        if self.rw_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentQueryDepKey: {
                            name: ConfigurableFirestoreReadOnlyDocument(
                                config=_document_config_to_read_only(config)
                            )
                            for name, config in self.rw_documents.items()
                        },
                        DocumentCommandDepKey: {
                            name: ConfigurableFirestoreDocument(config=config)
                            for name, config in self.rw_documents.items()
                        },
                    }
                )
            )

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps.routed(
                    {
                        TransactionManagerDepKey: {
                            name: firestore_txmanager for name in self.tx
                        }
                    }
                )
            )

        return plain_deps.merge(doc_deps, tx_deps)
