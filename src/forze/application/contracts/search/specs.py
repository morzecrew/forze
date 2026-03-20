from typing import NotRequired, TypedDict

import attrs
from pydantic import BaseModel

from .types import SearchIndexMode

# ----------------------- #


class SearchGroupSpec(TypedDict):
    """Configuration for a named search-field group and its weighting."""

    name: str
    """Group identifier."""

    weight: NotRequired[float]
    """Relative weight applied to fields in this group during ranking."""


# ....................... #


class SearchFieldSpec(TypedDict):
    """Configuration for a single field within a search index."""

    path: str
    """Dot-separated path to the document field."""

    group: NotRequired[str]
    """Optional group this field belongs to."""

    weight: NotRequired[float]
    """Relative weight for ranking; overrides the group weight when set."""


# ....................... #


class SearchFuzzySpec(TypedDict, total=False):
    """Fuzzy matching configuration for a search index."""

    enabled: bool
    """Whether fuzzy matching is enabled."""

    max_distance_ratio: float
    """Maximum edit-distance ratio (0.0–1.0) for fuzzy matches."""

    prefix_length: int
    """Number of leading characters that must match exactly."""


# ....................... #


class SearchIndexSpec(TypedDict):
    """Full specification of a search index: fields, groups, and behavior."""

    fields: list[SearchFieldSpec]
    """Fields included in this index."""

    groups: NotRequired[list[SearchGroupSpec]]
    """Optional field groups for grouped weighting."""

    default_group: NotRequired[str]
    """Group applied to fields that do not specify one explicitly."""

    mode: NotRequired[SearchIndexMode]
    """Indexing mode (e.g. full-text search engine variant)."""

    fuzzy: NotRequired[SearchFuzzySpec]
    """Fuzzy matching settings."""

    source: NotRequired[str]
    """Optional source identifier for multi-source indexes."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSpec[M: BaseModel]:
    """Specification binding a search namespace to its model and index definitions."""

    namespace: str
    """Logical search namespace."""

    model: type[M]
    """Pydantic model class for searchable documents."""

    indexes: dict[str, SearchIndexSpec]
    """Named index definitions for the namespace."""

    default_index: str | None = None
    """Index used when no explicit index name is provided in a query."""
