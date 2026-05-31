"""Firestore integration for Forze."""

from ._compat import require_firestore

require_firestore()

# ....................... #

from .execution import (
    FirestoreClientDepKey,
    FirestoreDepsModule,
    FirestoreDocumentConfig,
    FirestoreReadOnlyDocumentConfig,
    firestore_lifecycle_step,
    routed_firestore_lifecycle_step,
)
from .kernel.client import (
    FirestoreClient,
    FirestoreClientPort,
    FirestoreRoutingCredentials,
    RoutedFirestoreClient,
)
from .kernel.relation import (
    RelationSpec,
    coerce_relation_spec,
    is_static_relation,
    relations_match,
    resolve_firestore_collection,
)

# ----------------------- #

__all__ = [
    "FirestoreDepsModule",
    "FirestoreClient",
    "FirestoreClientPort",
    "RoutedFirestoreClient",
    "FirestoreRoutingCredentials",
    "FirestoreClientDepKey",
    "firestore_lifecycle_step",
    "routed_firestore_lifecycle_step",
    "FirestoreDocumentConfig",
    "FirestoreReadOnlyDocumentConfig",
    "RelationSpec",
    "coerce_relation_spec",
    "is_static_relation",
    "relations_match",
    "resolve_firestore_collection",
]
