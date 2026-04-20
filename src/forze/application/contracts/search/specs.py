from typing import Any, Mapping, Sequence, TypedDict

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
    """Specification for simple search (one index)."""

    model_type: type[M]
    """Pydantic model class for searchable documents."""

    fields: Sequence[str] = attrs.field(validator=attrs.validators.min_len(1))
    """Indexed fields."""

    default_weights: Mapping[str, float] | None = attrs.field(default=None)
    """Default weights for fields."""

    fuzzy: SearchFuzzySpec | None = attrs.field(default=None)
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
class HubSearchSpec[M: BaseModel](BaseSpec):
    """Hub (junction) search (homogeneous search)."""

    model_type: type[M]
    """Pydantic read model for hub rows."""

    members: Sequence[SearchSpec[Any]] = attrs.field(
        validator=attrs.validators.min_len(2),
    )
    """At least two :class:`SearchSpec` members."""

    default_member_weights: Mapping[str, float] | None = attrs.field(default=None)
    """Default weights for hub members."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = [member.name for member in self.members]

        if len(names) != len(set(names)):
            raise CoreError(
                "Each hub search member must use a SearchSpec with a distinct name."
            )

        if self.default_member_weights:
            for member in self.members:
                if member.name not in self.default_member_weights:
                    raise CoreError(
                        f"Default weight for unknown search field '{member.name}'."
                    )

                w = self.default_member_weights[member.name]

                if w < 0 or w > 1:
                    raise CoreError(
                        f"Default weight for search field '{member.name}' should be between 0.0 and 1.0."
                    )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FederatedSearchSpec[X: BaseModel](BaseSpec):
    """Federated search specification (heterogeneous search)."""

    members: Sequence[SearchSpec[Any]] = attrs.field(
        validator=attrs.validators.min_len(2),
    )
    """At least two :class:`SearchSpec` members."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = [member.name for member in self.members]

        if len(names) != len(set(names)):
            raise CoreError(
                "Each federated search member must use a SearchSpec with a distinct name."
            )
