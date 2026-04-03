from typing import Sequence, TypedDict

import attrs
from pydantic import BaseModel

from forze.base.errors import CoreError

from ..base import BaseSpec

# ----------------------- #


class SearchFuzzySpec(TypedDict, total=False):
    """Fuzzy matching configuration for a search index."""

    max_distance_ratio: float
    """Maximum edit-distance ratio (0.0–1.0) for fuzzy matches."""

    prefix_length: int
    """Number of leading characters that must match exactly."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSpec[M: BaseModel](BaseSpec):
    """Specification for simple search (one index).

    #! TODO: add a proper description
    """

    model_type: type[M]
    """Pydantic model class for searchable documents."""

    fields: Sequence[str] = attrs.field(validator=attrs.validators.min_len(1))
    """Indexed fields."""

    default_weights: dict[str, float] | None = None
    """Default weights for fields."""

    fuzzy: SearchFuzzySpec | None = None
    """Fuzzy matching configuration."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.fields) != len(set(self.fields)):
            raise CoreError("Search fields must be unique.")

        if not self.default_weights:
            return

        for f, w in self.default_weights.items():
            if f not in self.fields:
                raise CoreError(f"Default weight for unknown search field '{f}'.")

            if w < 0 or w > 1:
                raise CoreError(
                    f"Default weight for search field '{f}' should be between 0.0 and 1.0."
                )

        if not all(f in self.default_weights for f in self.fields):
            raise CoreError("Default weights must be provided for all search fields.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FederatedSearchSpec[M: BaseModel]:
    """Specification for federated search (many indexes)."""

    name: str
    """Logical search namespace name."""

    model_type: type[M]
    """Pydantic model class for searchable documents."""

    #! container with simple search specs ????
