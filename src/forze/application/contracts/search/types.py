from typing import Sequence, TypedDict

# ----------------------- #
#! Switch to support `mode` with values: fulltext, phrase, prefix, exact, fuzzy
#! instead of separate `fuzzy` bool flag


class SearchOptions(TypedDict, total=False):
    """Optional tuning parameters for search backends."""

    fuzzy: bool
    """Whether fuzzy matching is enabled."""

    weights: dict[str, float]
    """Field weights (between 0.0 and 1.0). If field weight is not specified, it will be set to 0.0."""

    fields: Sequence[str]
    """Simple alternative to weights for specifying fields to search on.

    For specified fields weights will be set to 1.0, for other fields weights will be set to 0.0.
    Ignored if weights are provided.
    """
