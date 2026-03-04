from typing import NotRequired, Optional, TypedDict

import attrs
from pydantic import BaseModel

from .types import SearchIndexMode

# ----------------------- #


class SearchGroupSpec(TypedDict):
    name: str

    # not required fields
    weight: NotRequired[float]


# ....................... #


class SearchFieldSpec(TypedDict):
    path: str

    # not required fields
    group: NotRequired[str]
    weight: NotRequired[float]


# ....................... #


class SearchFuzzySpec(TypedDict, total=False):
    enabled: bool
    max_distance_ratio: float
    prefix_length: int


# ....................... #


class SearchIndexSpec(TypedDict):
    fields: list[SearchFieldSpec]

    # not required fields
    groups: NotRequired[dict[str, SearchGroupSpec]]
    default_group: NotRequired[str]
    mode: NotRequired[SearchIndexMode]
    fuzzy: NotRequired[SearchFuzzySpec]
    source: NotRequired[str]


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSpec[M: BaseModel]:
    namespace: str
    model: type[M]
    indexes: dict[str, SearchIndexSpec]
    default_index: Optional[str] = None
