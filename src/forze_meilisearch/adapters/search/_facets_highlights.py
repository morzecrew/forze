"""Facet & highlight planning/extraction for Meilisearch search.

Maps the contract-resolved *logical* facet/highlight request onto Meilisearch's native
``facetDistribution`` and ``_formatted`` (``attributesToHighlight``), then back to logical
field names. Request validation lives in the contract (``resolve_facet_fields`` /
``resolve_highlight``) so it is identical across backends.
"""

from typing import Any, Sequence

import attrs

from forze.application.contracts.search import (
    FacetBucket,
    FacetResults,
    HitHighlights,
    SearchOptions,
    SearchSpec,
    facet_size_of,
    reject_nested_highlight_fields,
    resolve_facet_fields,
    resolve_highlight,
)

from .base import MeilisearchSearchGateway

# ----------------------- #


@attrs.define(frozen=True, slots=True)
class FacetPlan:
    """Resolved facet request mapped onto Meilisearch attribute names."""

    physical_fields: list[str]
    phys_to_logical: dict[str, str]
    size: int


# ....................... #


@attrs.define(frozen=True, slots=True)
class HighlightPlan:
    """Resolved highlight request mapped onto Meilisearch attribute names."""

    physical_fields: list[str]
    phys_to_logical: dict[str, str]
    pre_tag: str
    post_tag: str


# ....................... #


def _phys_map(
    gw: MeilisearchSearchGateway[Any],
    logical: Sequence[str],
) -> tuple[list[str], dict[str, str]]:
    physical = gw.physical_paths(logical)

    return list(physical), dict(zip(physical, logical, strict=True))


# ....................... #


def plan_facets(
    gw: MeilisearchSearchGateway[Any],
    spec: SearchSpec[Any],
    options: SearchOptions | None,
) -> FacetPlan | None:
    """Validate + map the facet request, or ``None`` when none was requested."""

    logical = resolve_facet_fields(spec, options)

    if not logical:
        return None

    physical, phys_to_logical = _phys_map(gw, logical)

    return FacetPlan(
        physical_fields=physical,
        phys_to_logical=phys_to_logical,
        size=facet_size_of(options),
    )


# ....................... #


def plan_highlights(
    gw: MeilisearchSearchGateway[Any],
    spec: SearchSpec[Any],
    options: SearchOptions | None,
) -> HighlightPlan | None:
    """Validate + map the highlight request, or ``None`` when none was requested."""

    resolved = resolve_highlight(spec, options)

    if resolved is None:
        return None

    logical, pre_tag, post_tag = resolved
    reject_nested_highlight_fields(logical, backend="Meilisearch")
    physical, phys_to_logical = _phys_map(gw, logical)

    return HighlightPlan(
        physical_fields=physical,
        phys_to_logical=phys_to_logical,
        pre_tag=pre_tag,
        post_tag=post_tag,
    )


# ....................... #


def extract_facets(result: Any, plan: FacetPlan) -> FacetResults:
    """Build :class:`FacetResults` from Meilisearch ``facetDistribution``.

    Meilisearch facet-distribution keys are strings (JSON object keys), so bucket values
    are returned as strings — the engine's native behavior. Buckets are ordered count-desc,
    value-asc, capped at ``plan.size``.
    """

    distribution: dict[str, Any] = dict(
        getattr(result, "facet_distribution", None) or {}
    )
    out: dict[str, tuple[FacetBucket, ...]] = {}

    for physical, logical in plan.phys_to_logical.items():
        raw: dict[str, Any] = dict(distribution.get(physical) or {})
        ordered = sorted(raw.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))

        out[logical] = tuple(
            FacetBucket(value=value, count=int(count))
            for value, count in ordered[: plan.size]
        )

    return out


# ....................... #


def extract_highlights(
    hits_raw: Sequence[dict[str, Any]],
    plan: HighlightPlan,
) -> list[HitHighlights]:
    """Per-hit highlighted fragments from Meilisearch ``_formatted`` (index-aligned).

    A field is included only when its formatted value actually carries the marker (i.e.
    a match), mirroring the reference-oracle semantics; a hit with no matches maps to ``{}``.
    """

    out: list[HitHighlights] = []

    for hit in hits_raw:
        formatted: dict[str, Any] = dict(hit.get("_formatted") or {})
        marked: dict[str, tuple[str, ...]] = {}

        for physical in plan.physical_fields:
            if fragments := _fragments(formatted.get(physical), plan.pre_tag):
                marked[plan.phys_to_logical[physical]] = fragments

        out.append(marked)

    return out


# ....................... #


def _fragments(value: Any, pre_tag: str) -> tuple[str, ...]:
    """Marker-bearing fragments of a Meilisearch ``_formatted`` field value."""

    if isinstance(value, str):
        return (value,) if pre_tag in value else ()

    if isinstance(value, (list, tuple)):
        items: list[Any] = list(value)  # pyright: ignore[reportUnknownArgumentType]
        return tuple(v for v in items if isinstance(v, str) and pre_tag in v)

    return ()
