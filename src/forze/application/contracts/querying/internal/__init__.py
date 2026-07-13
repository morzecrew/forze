"""Query DSL: AST nodes, parser, and value caster."""

from .aggregate import (
    AggregateComputedField,
    AggregatesExpressionParser,
    GroupField,
    GroupKey,
    GroupTrunc,
    ParsedAggregates,
)
from .cast import QueryValueCaster
from .matching import compile_filter, evaluate_filter, value_at_path
from .nodes import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
    elem_inner_is_scalar,
)
from .parse import QueryFilterExpressionParser, QueryFilterLimits
from .text_pattern import like_pattern_to_regex, validate_text_pattern

# ----------------------- #

__all__ = [
    "AggregateComputedField",
    "AggregatesExpressionParser",
    "GroupKey",
    "GroupField",
    "GroupTrunc",
    "ParsedAggregates",
    "QueryFilterExpressionParser",
    "QueryFilterLimits",
    "QueryValueCaster",
    "evaluate_filter",
    "compile_filter",
    "value_at_path",
    "ELEM_SCALAR_FIELD",
    "QueryAnd",
    "QueryCompare",
    "QueryElem",
    "QueryExpr",
    "QueryField",
    "QueryNot",
    "QueryOr",
    "elem_inner_is_scalar",
    "like_pattern_to_regex",
    "validate_text_pattern",
]
