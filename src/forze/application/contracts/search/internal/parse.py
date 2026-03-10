from pydantic import BaseModel

from forze.base.errors import CoreError

from ..specs import (
    SearchFieldSpec,
    SearchFuzzySpec,
    SearchGroupSpec,
    SearchIndexSpec,
    SearchSpec,
)
from .specs import (
    SearchFieldSpecInternal,
    SearchFuzzySpecInternal,
    SearchGroupSpecInternal,
    SearchIndexSpecInternal,
    SearchSpecInternal,
)

# ----------------------- #


def _parse_group_spec(spec: SearchGroupSpec) -> SearchGroupSpecInternal:
    return SearchGroupSpecInternal(
        name=spec["name"],
        weight=spec.get("weight", 1.0),
        hints=spec.get("hints", {}),  # type: ignore[arg-type]
    )


# ....................... #


def _parse_field_spec(spec: SearchFieldSpec) -> SearchFieldSpecInternal:
    return SearchFieldSpecInternal(
        path=spec["path"],
        group=spec.get("group"),
        weight=spec.get("weight"),
        hints=spec.get("hints", {}),  # type: ignore[arg-type]
    )


# ....................... #


def _parse_fuzzy_spec(spec: SearchFuzzySpec) -> SearchFuzzySpecInternal:
    return SearchFuzzySpecInternal(
        enabled=spec.get("enabled", False),
        max_distance_ratio=spec.get("max_distance_ratio"),
        prefix_length=spec.get("prefix_length"),
        hints=spec.get("hints", {}),  # type: ignore[arg-type]
    )


# ....................... #


def _parse_index_spec(
    spec: SearchIndexSpec,
    *,
    raise_if_no_sources: bool = False,
) -> SearchIndexSpecInternal:
    fields = [_parse_field_spec(field) for field in spec["fields"]]
    groups = [_parse_group_spec(group) for group in spec.get("groups", [])]
    fuzzy = _parse_fuzzy_spec(spec.get("fuzzy", {}))
    source = spec.get("source")

    if raise_if_no_sources and not source:
        raise CoreError("Index spec must have a source")

    return SearchIndexSpecInternal(
        fields=fields,
        groups=groups,
        default_group=spec.get("default_group"),
        mode=spec.get("mode", "fulltext"),
        fuzzy=fuzzy,
        source=source,
        hints=spec.get("hints", {}),  # type: ignore[arg-type]
    )


# ....................... #


def parse_search_spec[T: BaseModel](
    spec: SearchSpec[T],
    *,
    raise_if_no_sources: bool = False,
) -> SearchSpecInternal[T]:
    indexes = {
        name: _parse_index_spec(index, raise_if_no_sources=raise_if_no_sources)
        for name, index in spec.indexes.items()
    }

    return SearchSpecInternal(
        namespace=spec.namespace,
        model=spec.model,
        indexes=indexes,
        default_index=spec.default_index,
    )
