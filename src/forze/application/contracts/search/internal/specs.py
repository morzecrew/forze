from functools import cached_property
from typing import Optional

import attrs
from more_itertools import first
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

from ..types import SearchIndexMode, SearchOptions

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchGroupSpecInternal:
    """Semantic ranking group"""

    name: str
    weight: float = 1.0
    hints: JsonDict = attrs.field(factory=dict)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.name.strip():
            raise CoreError("Group name cannot be empty")

        if self.weight < 0.0:
            raise CoreError("Group weight cannot be negative")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFieldSpecInternal:
    """Indexed field specification"""

    path: str
    group: Optional[str] = None
    weight: Optional[float] = None
    hints: JsonDict = attrs.field(factory=dict)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.path_safe:
            raise CoreError("Field path cannot be empty")

        if self.weight is not None and self.weight < 0.0:
            raise CoreError("Field weight cannot be negative")

        if self.group is not None and not self.group.strip():
            raise CoreError("Group name cannot be empty")

    # ....................... #

    @cached_property
    def path_safe(self) -> str:
        return self.path.strip()


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFuzzySpecInternal:
    enabled: bool = False
    max_distance_ratio: Optional[float] = None
    prefix_length: Optional[int] = None
    hints: JsonDict = attrs.field(factory=dict)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_distance_ratio is not None:
            if self.max_distance_ratio < 0.0:
                raise CoreError("Max distance ratio cannot be negative")

            if self.max_distance_ratio > 1.0:
                raise CoreError("Max distance ratio cannot be greater than 1.0")

        if self.prefix_length is not None and self.prefix_length < 0:
            raise CoreError("Prefix length cannot be negative")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchIndexSpecInternal:
    fields: list[SearchFieldSpecInternal]
    groups: list[SearchGroupSpecInternal] = attrs.field(factory=list)
    default_group: Optional[str] = None
    mode: SearchIndexMode = "fulltext"
    fuzzy: Optional[SearchFuzzySpecInternal] = None
    source: Optional[str] = None
    hints: JsonDict = attrs.field(factory=dict)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.fields) < 1:
            raise CoreError("At least one field is required")

        field_paths = [field.path_safe for field in self.fields]

        if len(field_paths) != len(set(field_paths)):
            raise CoreError("Field paths must be unique")

        group_names = [group.name for group in self.groups]

        if self.default_group is not None and self.default_group not in group_names:
            raise CoreError(f"Default group '{self.default_group}' not found in groups")

        for f in self.fields:
            if f.group is not None and f.group not in group_names:
                raise CoreError(
                    f"Field '{f.path_safe}' references unknown group '{f.group}'"
                )

            if f.group is None and self.default_group is None and self.groups:
                raise CoreError(
                    f"Field '{f.path_safe}' has no group and default_group is not set"
                )

    # ....................... #

    @cached_property
    def groups_dict(self) -> dict[str, SearchGroupSpecInternal]:
        return {group.name: group for group in self.groups}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSpecInternal[M: BaseModel]:
    namespace: str
    model: type[M]
    indexes: dict[str, SearchIndexSpecInternal]
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

    def pick_index(
        self,
        options: Optional[SearchOptions] = None,
    ) -> tuple[str, SearchIndexSpecInternal]:
        options = options or {}
        index = options.get("use_index", self.stable_default_index)

        if index not in self.indexes:
            raise CoreError(f"Index `{index}` not found")

        return index, self.indexes[index]
