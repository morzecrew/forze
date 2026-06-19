"""Build Meilisearch search request parameters from Forze search inputs."""

from __future__ import annotations

from typing import Any

from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.search import (
    PhraseCombine,
    SearchOptions,
    SearchSpec,
    calculate_effective_field_weights,
)
from forze_meilisearch.adapters._logger import logger

# ----------------------- #


def build_search_query_string(
    terms: tuple[str, ...],
    *,
    combine: PhraseCombine,
) -> str:
    if not terms:
        return ""

    # Strip the phrase delimiter from each term so an embedded ``"`` cannot
    # break phrase boundaries or split the query unexpectedly. (A leading ``-``
    # remains a Meilisearch negation operator -- documented behaviour.)
    safe = [t.replace('"', "") for t in terms]

    if combine == "all":
        return " ".join(f'"{t}"' for t in safe)

    return " ".join(safe)


def attributes_to_search_on(
    spec: SearchSpec[Any],
    options: SearchOptions | None,
    field_map: dict[str, str],
) -> list[str] | None:
    weights = calculate_effective_field_weights(spec, options)
    active = [f for f, w in weights.items() if w > 0.0]

    if spec.default_weights and not (options or {}).get("weights") and not (
        options or {}
    ).get("fields"):
        logger.warning(
            "meilisearch_default_weights_best_effort",
            message=(
                "SearchSpec.default_weights are mapped best-effort to attributesToSearchOn; "
                "Meilisearch does not support per-field FTS weights like Postgres."
            ),
        )

    if not active:
        return None

    return [field_map.get(f, f) for f in active]


def build_sort(spec_sorts: list[tuple[str, str]]) -> list[str] | None:
    if not spec_sorts:
        return None

    return [f"{field}:{direction}" for field, direction in spec_sorts]


def render_user_sorts(
    sorts: QuerySortExpression | None,
    field_map: dict[str, str],
) -> list[tuple[str, str]]:
    if not sorts:
        return []

    out: list[tuple[str, str]] = []

    for field, direction in sorts.items():
        d = str(direction).lower()
        phys = field_map.get(field, field)
        out.append((phys, d))

    return out


def merge_filter_strings(*parts: str | None) -> str | None:
    active = [p for p in parts if p]

    if not active:
        return None

    if len(active) == 1:
        return active[0]

    return "(" + ") AND (".join(active) + ")"
