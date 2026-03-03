from typing import Literal, TypedDict

# ----------------------- #

SearchIndexMode = Literal["fulltext", "phrase", "prefix", "exact"]
SearchFieldType = Literal["text", "keyword"]
SearchWeightsPolicy = Literal["strict", "groups", "fields", "multiply"]

# ....................... #


class SearchFuzzyOptions(TypedDict, total=False):
    enabled: bool
    max_distance_ratio: float
    prefix_length: int


# ....................... #


class SearchWeightOptions(TypedDict, total=False):
    policy: SearchWeightsPolicy
    groups: dict[str, float]
    fields: dict[str, float]


#!? fields options ? mb makes sense to select "fields to search on" or specify weights instead
#! or 'simplified' way to specify fields to search on (transform to weights with 0 and 1 internally)

# ....................... #


class SearchOptions(TypedDict, total=False):
    """Optional tuning parameters for search backends."""

    use_index: str
    mode: SearchIndexMode
    fuzzy: SearchFuzzyOptions
    weights: SearchWeightOptions
    language: str
