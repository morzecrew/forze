from .deps import (
    FederatedSearchQueryDepKey,
    FederatedSearchQueryDepPort,
    HubSearchQueryDepKey,
    HubSearchQueryDepPort,
    SearchCommandDepKey,
    SearchCommandDepPort,
    SearchQueryDepKey,
    SearchQueryDepPort,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotDepPort,
)
from .models import FederatedSearchReadModel
from .ports import SearchCommandPort, SearchQueryPort, SearchResultSnapshotPort
from .specs import (
    FederatedSearchMemberSpec,
    FederatedSearchSpec,
    HubSearchSpec,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from .types import (
    PhraseCombine,
    ResultSnapshotMode,
    SearchOptions,
    SearchResultSnapshotOptions,
)
from .utils import effective_phrase_combine, normalize_search_queries
from .value_objects import SearchResultSnapshotMeta

# ----------------------- #

__all__ = [
    "PhraseCombine",
    "ResultSnapshotMode",
    "SearchSpec",
    "FederatedSearchMemberSpec",
    "HubSearchSpec",
    "SearchOptions",
    "SearchResultSnapshotMeta",
    "SearchResultSnapshotPort",
    "SearchResultSnapshotSpec",
    "SearchResultSnapshotOptions",
    "SearchResultSnapshotDepKey",
    "SearchResultSnapshotDepPort",
    "effective_phrase_combine",
    "SearchQueryPort",
    "SearchCommandPort",
    "SearchQueryDepKey",
    "HubSearchQueryDepKey",
    "SearchCommandDepKey",
    "SearchCommandDepPort",
    "SearchQueryDepPort",
    "HubSearchQueryDepPort",
    "FederatedSearchQueryDepKey",
    "FederatedSearchQueryDepPort",
    "FederatedSearchSpec",
    "FederatedSearchReadModel",
    "normalize_search_queries",
]
