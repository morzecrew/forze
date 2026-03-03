from __future__ import annotations

from functools import cached_property
from typing import Optional

import attrs
from more_itertools import first

from forze.base.errors import CoreError, NotFoundError
from forze.base.primitives import JsonDict

from .types import SearchFieldType, SearchIndexMode, SearchRankingStrategy

# ----------------------- #
#! Link model to index or search


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSpec:
    indexes: dict[str, SearchIndexSpec] = attrs.field(factory=dict)
    default_index: Optional[str] = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.indexes:
            raise CoreError("At least one index is required")

        if self.default_index is not None and self.default_index not in self.indexes:
            raise CoreError(
                f"Default index `{self.default_index}` not found in indexes"
            )

    # ....................... #

    @cached_property
    def stable_default_index(self) -> str:
        return self.default_index or first(self.indexes.keys())

    # ....................... #

    def get_index(self, name: Optional[str] = None) -> SearchIndexSpec:
        idx = name or self.stable_default_index

        if idx not in self.indexes:
            raise NotFoundError(f"Index `{idx}` not found")

        return self.indexes[idx]


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchIndexSpec:
    fields: list[SearchFieldSpec]
    mode: SearchIndexMode = "fulltext"
    fuzzy: Optional[SearchFuzzySpec] = None
    ranking: Optional[SearchRankingSpec] = None
    hints: JsonDict = attrs.field(factory=dict)
    source: Optional[str] = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.fields) < 1:
            raise CoreError("At least one field is required")

        field_paths = [field.path_safe for field in self.fields]

        if len(field_paths) != len(set(field_paths)):
            raise CoreError("Field paths must be unique")

        if self.source is not None and not self.source.strip():
            raise CoreError("Source cannot be empty if provided explicitly")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFieldSpec:
    path: str
    weight: float = 1.0
    type: SearchFieldType = "text"
    analyzer: Optional[str] = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.path_safe:
            raise CoreError("Field path cannot be empty")

        if self.weight < 0.0:
            raise CoreError("Weight must be greater than or equal to 0.0")

    # ....................... #

    @cached_property
    def path_safe(self) -> str:
        return self.path.strip()


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFuzzySpec:
    enabled: bool = False
    max_distance_ratio: Optional[float] = None
    prefix_length: Optional[int] = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_distance_ratio is not None:
            if self.max_distance_ratio < 0.0:
                raise CoreError(
                    "Max distance ratio must be greater than or equal to 0.0"
                )

            if self.max_distance_ratio > 1.0:
                raise CoreError("Max distance ratio must be less than or equal to 1.0")

        if self.prefix_length is not None and self.prefix_length < 0:
            raise CoreError("Prefix length must be greater than or equal to 0")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchRankingSpec:
    strategy: SearchRankingStrategy = "native"
    weights_overridable: bool = True
