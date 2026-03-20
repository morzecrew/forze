"""Utilities for mapping search spec groups to Postgres FTS weight letters."""

from typing import Literal, Optional

from forze.application.contracts.search import SearchIndexSpecInternal, SearchOptions
from forze_postgres.kernel._logger import logger

# ----------------------- #

FtsGroupLetter = Literal["A", "B", "C", "D"]
"""One of the four Postgres FTS weight labels."""

# ....................... #


def fts_map_groups(spec: SearchIndexSpecInternal) -> dict[str, FtsGroupLetter]:
    """Map search spec group names to FTS weight letters ordered by descending weight.

    Postgres supports at most four weight letters (``A``–``D``).  Groups beyond
    the first four are dropped with a warning.

    :param spec: Internal search index specification.
    :returns: Mapping of group name to weight letter.
    """

    if not spec.groups:
        return {"__default__": "A"}

    ordered = sorted(spec.groups, key=lambda g: g.weight, reverse=True)

    if len(ordered) > 4:
        logger.warning(
            "FTS index spec contains '%s' groups, but Postgres only supports 4 weights (A, B, C, D). "
            "Groups after the first 4 (by weight) will be ignored.",
            len(ordered),
        )
        ordered = ordered[:4]

    letters: list[FtsGroupLetter] = ["A", "B", "C", "D"]

    return {g.name: letters[i] for i, g in enumerate(ordered)}


# ....................... #


def fts_rank_weights_array(
    spec: SearchIndexSpecInternal,
    options: Optional[SearchOptions] = None,
) -> list[float]:
    """Build the four-element weight array for ``ts_rank_cd`` in ``[D, C, B, A]`` order.

    Base weights come from the spec groups; per-request overrides in *options*
    take precedence.

    :param spec: Internal search index specification.
    :param options: Optional per-request search options with weight overrides.
    :returns: Four-element float list ordered ``[D, C, B, A]``.
    """

    options = options or {}
    group_letters = fts_map_groups(spec)

    # FTS default weight order: D, C, B, A
    weights: dict[FtsGroupLetter, float] = {"A": 1.0, "B": 0.4, "C": 0.2, "D": 0.1}

    for group in spec.groups:
        letter = group_letters.get(group.name)

        if letter:
            weights[letter] = group.weight

    ov_weights = options.get("weights", {}).get("groups", {})

    if ov_weights:
        for name, weight in ov_weights.items():
            letter = group_letters.get(name)

            if letter:
                weights[letter] = weight

    return [weights["D"], weights["C"], weights["B"], weights["A"]]
