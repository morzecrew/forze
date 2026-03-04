from typing import Literal, Optional

from forze.application.contracts.search import SearchIndexSpecInternal, SearchOptions

# ----------------------- #

FtsGroupLetter = Literal["A", "B", "C", "D"]


def fts_map_groups(spec: SearchIndexSpecInternal) -> dict[str, FtsGroupLetter]:
    if not spec.groups:
        return {"__default__": "A"}

    ordered = sorted(spec.groups, key=lambda g: g.weight, reverse=True)

    if len(ordered) > 4:
        #! TODO: add warning
        ordered = ordered[:4]

    letters: list[FtsGroupLetter] = ["A", "B", "C", "D"]

    return {g.name: letters[i] for i, g in enumerate(ordered)}


# ....................... #


def fts_rank_weights_array(
    spec: SearchIndexSpecInternal,
    options: Optional[SearchOptions] = None,
) -> list[float]:
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
