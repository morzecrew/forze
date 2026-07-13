"""Private tenancy warning descriptors for Firestore deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import FirestoreDocumentConfig, FirestoreReadOnlyDocumentConfig

# ----------------------- #

FIRESTORE_DOCUMENT_RO_WARNING = IntegrationRouteWarning[FirestoreReadOnlyDocumentConfig](
    kind="document",
    tenant_aware=lambda config: config.tenant_aware,
    relation_fields=lambda config: [("read", config.read)],
)

FIRESTORE_DOCUMENT_RW_WARNING = IntegrationRouteWarning[FirestoreDocumentConfig](
    kind="document",
    tenant_aware=lambda config: config.tenant_aware,
    relation_fields=lambda config: [
        ("read", config.read),
        ("write", config.write),
        ("history", config.history),
    ],
)
