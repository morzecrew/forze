"""Build Meilisearch search request parameters from Forze search inputs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from forze.application.contracts.querying import (
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QuerySortExpression,
    parse_sort_value,
)
from forze.application.contracts.search import (
    PhraseCombine,
    SearchOptions,
    SearchSpec,
    calculate_effective_field_weights,
)
from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD
from forze_meilisearch.adapters._logger import logger

if TYPE_CHECKING:
    from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig

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

    if (
        spec.default_weights
        and not (options or {}).get("weights")
        and not (options or {}).get("fields")
    ):
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


def places_nulls(value: Any) -> bool:
    """Whether a sort value names a null placement, which Meilisearch cannot honour."""

    return isinstance(value, Mapping) and value.get("nulls") is not None  # pyright: ignore[reportUnknownMemberType]


# ....................... #


def sort_attribute(field: str, config: MeilisearchSearchConfig) -> str:
    """The index attribute a logical sort field reads: its mapped name, the id as the key.

    A document carries its id under the primary key, which is the attribute the index
    declares sortable by default.
    """

    if field == ID_FIELD:
        return config.primary_key

    return (config.field_map or {}).get(field, field)


# ....................... #


def sortable_attributes(spec: SearchSpec[Any], config: MeilisearchSearchConfig) -> list[str]:
    """The attributes ``ensure_index`` declares sortable for *spec* under *config*.

    Pinned ``sortable_attributes`` as given; otherwise the primary key, the searchable fields
    and whatever the spec orders an unsorted page by.
    """

    pinned = config.sortable_attributes
    fields = pinned if pinned is not None else [ID_FIELD, *spec.fields, *(spec.default_sort or ())]

    return list(dict.fromkeys(sort_attribute(f, config) for f in fields))


# ....................... #


def render_user_sorts(
    sorts: QuerySortExpression | None,
    config: MeilisearchSearchConfig,
) -> list[tuple[str, str]]:
    if not sorts:
        return []

    out: list[tuple[str, str]] = []

    for field, value in sorts.items():
        # Through the canonical parser first, which reads the ``{"dir": ...}`` form and refuses
        # a malformed value as such; only then is a well-formed placement unsupported here.
        d, _ = parse_sort_value(value, field=field)

        if places_nulls(value):
            raise exc.precondition(
                f"Meilisearch cannot place nulls on a sort; drop 'nulls' from {field!r}.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        out.append((sort_attribute(field, config), d))

    return out


def merge_filter_strings(*parts: str | None) -> str | None:
    active = [p for p in parts if p]

    if not active:
        return None

    if len(active) == 1:
        return active[0]

    return "(" + ") AND (".join(active) + ")"
