"""Shared checks for a search aggregate request, ahead of any backend work."""

from typing import Any

from forze.base.exceptions import exc

from ..querying import (
    AggregatesExpression,
    QueryFilterExpressionParser,
    collect_aggregate_filter_expressions,
    validate_aggregatable_fields,
    validate_runtime_filter_fields,
)
from ..querying.capabilities import UNSUPPORTED_QUERY_FEATURE_CODE
from .specs import SearchSpec
from .types import SearchOptions

# ----------------------- #


def validate_search_aggregates(
    spec: SearchSpec[Any],
    aggregates: AggregatesExpression,
    options: SearchOptions | None,
    *,
    parser: QueryFilterExpressionParser | None = None,
) -> None:
    """Refuse a search aggregate no backend should run, the same way on every backend.

    Facets and highlights describe hits, and an aggregate returns groups, so asking for them
    is refused rather than ignored; so is ``max_candidates``, which would cut the matched set an
    aggregate measures whole. Groups, measures and per-metric filters may name only
    :attr:`~.SearchSpec.aggregatable_fields`: a lenient field has no stored value, and a
    field-encrypted one holds ciphertext.
    """

    opts = options or {}

    if opts.get("facets") or opts.get("highlight"):
        raise exc.precondition(
            f"Search spec {spec.name!r}: facets and highlight describe hits, and an "
            "aggregate returns groups; leave them out of an aggregate's options.",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
        )

    if opts.get("max_candidates") is not None:
        raise exc.precondition(
            f"Search spec {spec.name!r}: an aggregate measures every match, so it takes no "
            "max_candidates cap.",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
        )

    validate_aggregatable_fields(
        aggregates,
        allowed=spec.aggregatable_fields,
        spec_name=str(spec.name),
        parser=parser,
    )

    sealed = spec.stored_read_fields - spec.aggregatable_fields

    for expression in collect_aggregate_filter_expressions(aggregates, parser=parser):
        validate_runtime_filter_fields(
            expression,
            model=spec.model_type,
            materialized=spec.materialized,
            lenient=spec.resolved_lenient_read_fields,
            encrypted=sealed,
            parser=parser,
        )
