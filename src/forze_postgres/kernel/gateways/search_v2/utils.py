import re
from typing import Literal, Optional

from forze.application.contracts.search import SearchIndexSpec, SearchOptions

# ----------------------- #

_INDEXDEF_PARENS_RE = re.compile(
    r"using\s+\w+\s*\((.*)\)\s*(where\s+.*)?$", re.IGNORECASE | re.DOTALL
)

# ....................... #


def extract_index_expr_from_indexdef(indexdef: str) -> Optional[str]:
    """
    Try to extract the single (...) expression part from:
      CREATE INDEX ... USING gin (<expr>) ...
    This is intentionally simple and may fail for exotic definitions.
    """

    m = _INDEXDEF_PARENS_RE.search(indexdef.strip())

    if not m:
        return None

    expr = m.group(1).strip()

    return expr or None


# ....................... #

FtsGroupLetter = Literal["A", "B", "C", "D"]


def fts_map_groups(spec: SearchIndexSpec) -> dict[str, FtsGroupLetter]:
    if not spec.groups:
        return {"__default__": "A"}

    ordered = sorted(spec.groups.values(), key=lambda g: g.weight, reverse=True)

    if len(ordered) > 4:
        #! TODO: add warning
        ordered = ordered[:4]

    letters: list[FtsGroupLetter] = ["A", "B", "C", "D"]

    return {g.name: letters[i] for i, g in enumerate(ordered)}


# ....................... #


def fts_rank_weights_array(
    spec: SearchIndexSpec,
    options: Optional[SearchOptions] = None,
) -> list[float]:
    options = options or {}
    group_letters = fts_map_groups(spec)

    # FTS default weight order: D, C, B, A
    weights: dict[FtsGroupLetter, float] = {"A": 1.0, "B": 0.4, "C": 0.2, "D": 0.1}

    for name, group in spec.groups.items():
        letter = group_letters.get(name)

        if letter:
            weights[letter] = group.weight

    ov_weights = options.get("weights", {}).get("groups", {})

    if ov_weights:
        for name, weight in ov_weights.items():
            letter = group_letters.get(name)

            if letter:
                weights[letter] = weight

    return [weights["D"], weights["C"], weights["B"], weights["A"]]
