"""Shared checks for a search aggregate request, ahead of any backend work."""

from typing import Any, cast

from forze.base.exceptions import exc

from ..querying import (
    AggregatesExpression,
    AggregatesExpressionParser,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryNot,
    QueryOr,
    collect_aggregate_filter_expressions,
    collect_filter_field_roots,
    validate_aggregatable_fields,
    validate_runtime_filter_fields,
)
from ..querying.capabilities import UNSUPPORTED_QUERY_FEATURE_CODE
from .specs import HubSearchSpec, SearchSpec
from .types import MultiSourceSearchOptions, SearchOptions

# ----------------------- #


def _reaches_into(path: str, columns: frozenset[str]) -> bool:
    """Whether *path* is a dotted path into one of *columns*."""

    return "." in path and path.split(".", 1)[0] in columns


def _paths_into(expr: QueryExpr, columns: frozenset[str]) -> frozenset[str]:
    """Paths in *expr* reaching inside one of *columns*: dotted, or element-quantified."""

    match expr:
        case QueryAnd(items) | QueryOr(items):
            return frozenset().union(*(_paths_into(item, columns) for item in items))

        case QueryNot(item):
            return _paths_into(item, columns)

        case QueryElem(path, _, _):
            # The element predicate is relative to the array, so only its path is a reference.
            return frozenset({path}) if path.split(".", 1)[0] in columns else frozenset()

        case QueryField(name, _, _):
            names = {name}

        case QueryCompare(left, _, right):
            names = {left, right}

        case _:
            return frozenset()

    return frozenset(n for n in names if _reaches_into(n, columns))


# ....................... #


def validate_search_aggregates(
    spec: SearchSpec[Any] | HubSearchSpec[Any],
    aggregates: AggregatesExpression,
    options: SearchOptions | None,
    *,
    parser: QueryFilterExpressionParser | None = None,
) -> None:
    """Refuse a search aggregate no backend should run, the same way on every backend.

    Facets and highlights describe hits, and an aggregate returns groups, so asking for them
    (``highlight={}`` included) is refused rather than ignored; so are ``max_candidates`` and
    a hub's ``merge_candidates``, which would cut the matched set an aggregate measures whole.
    Groups, measures and per-metric filters may name only
    :attr:`~.SearchSpec.aggregatable_fields`: a lenient field has no stored value, and a
    field-encrypted one holds ciphertext. A hub's
    :attr:`~.HubSearchSpec.extra_aggregatable_fields` are among them, but no model describes
    what such a column holds, so an aggregate names one by its own value: a dotted path into
    one, or a metric filter's element quantifier over one, is refused. The search's
    ``filters`` stay on the read model.
    """

    opts = cast("MultiSourceSearchOptions", options or {})

    # Any highlight value but an absent or ``False`` one is a request (``{}`` asks for the
    # defaults); an empty ``facets`` list asks for none.
    if opts.get("facets") or opts.get("highlight") not in (None, False):
        raise exc.precondition(
            f"Search spec {spec.name!r}: facets and highlight describe hits, and an "
            "aggregate returns groups; leave them out of an aggregate's options.",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
        )

    if opts.get("max_candidates") is not None or opts.get("merge_candidates") is not None:
        raise exc.precondition(
            f"Search spec {spec.name!r}: an aggregate measures every match, so it takes no "
            "max_candidates or merge_candidates cap.",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
        )

    validate_aggregatable_fields(
        aggregates,
        allowed=spec.aggregatable_fields,
        spec_name=str(spec.name),
        parser=parser,
    )

    sealed = spec.stored_read_fields - spec.aggregatable_fields
    extra = spec.extra_aggregatable_fields if isinstance(spec, HubSearchSpec) else frozenset()

    if extra:
        measured = AggregatesExpressionParser.parse(aggregates, filter_parser=parser)
        fields = [group.expr.field for group in measured.groups]
        fields += [field.field for field in measured.computed_fields if field.field is not None]

        if paths := sorted({field for field in fields if _reaches_into(field, extra)}):
            raise exc.precondition(
                f"Search spec {spec.name!r}: a group or measure reaches into extra "
                f"aggregatable column(s) through {paths}; group by or measure an extra column "
                "by its own value.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

    for expression in collect_aggregate_filter_expressions(aggregates, parser=parser):
        if forbidden := collect_filter_field_roots(expression, parser=parser) & sealed:
            # Deterministic ciphertext matches an equality filter, but a measure reads the
            # stored values, so a searchable field is refused here as a randomized one is.
            raise exc.precondition(
                f"Search spec {spec.name!r}: a metric filter names field-encrypted field(s) "
                f"{sorted(forbidden)}; an aggregate measures stored values, and these hold "
                "ciphertext.",
                code="field_not_aggregatable",
            )

        # An extra column is outside the read model, but aggregatable, so a metric filter
        # may name it as a measure does.
        validate_runtime_filter_fields(
            expression,
            model=spec.model_type,
            materialized=spec.materialized | extra,
            lenient=spec.resolved_lenient_read_fields,
            parser=parser,
        )

        if not extra:
            continue

        parsed = (
            parser.parse_filter(expression)
            if parser is not None
            else QueryFilterExpressionParser.parse(expression)
        )

        if inside := _paths_into(parsed, extra):
            raise exc.precondition(
                f"Search spec {spec.name!r}: a metric filter reaches into extra aggregatable "
                f"column(s) through {sorted(inside)}; filter an extra column by its own value.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )
