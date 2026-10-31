"""Postgres hub search adapter."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from types import GenericAlias
from typing import Any, Literal, cast, final
from uuid import UUID

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.querying import (
    OPERATOR_TYPE_MISMATCH_CODE,
    UNSUPPORTED_QUERY_FEATURE_CODE,
    AggregatesExpression,
    PaginationExpression,
    QueryAnd,
    QueryCompare,
    QueryExpr,
    QueryField,
    QueryFilterExpression,
    QueryNot,
    QueryOr,
    QuerySortExpression,
    collect_aggregate_filter_expressions,
    validate_query_field_types,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    MultiSourceSearchOptions,
    SearchCapabilities,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    facet_size_of,
    resolve_facet_fields,
    resolve_fusion,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from ....kernel.catalog.introspect import PostgresType
from ....kernel.gateways import PostgresGateway
from ....kernel.sql.query import PsycopgQueryRenderer, compose_aggregate_statement
from .._aggregate_run import execute_search_aggregate
from .._materialize_hits import search_trust_source
from .._offset_run import RankedOffsetPlan, execute_hub_ranked_offset_search
from .._pipeline_sql import SEARCH_SCORE_ALIAS
from .._port import PostgresSearchPortMixin
from .._search_count import resolve_ranked_approximate_total
from ._facets_highlights import attach_hub_highlights
from .constants import COMBO_ALIAS, HUB_INTERNAL_ROW_KEYS, HUB_RANK
from .cursor import HubSearchCursorMixin
from .plan import build_hub_search_plan, hub_members_weighted
from .runtime import HubLegRuntime

# ----------------------- #

_PYTHON_TYPE_OF: Mapping[str, type[Any]] = {
    "uuid": UUID,
    "bool": bool,
    "int2": int,
    "int4": int,
    "int8": int,
    "float4": float,
    "float8": float,
    "numeric": Decimal,
    "text": str,
    "varchar": str,
    "char": str,
    "bpchar": str,
    "citext": str,
    "name": str,
    "date": date,
    "timestamp": datetime,
    "timestamptz": datetime,
}
"""The Python type a metric filter's operators on an extra hub column are checked against,
by the column's base type. A column of a type missing here (``jsonb``, ``time``, ...) takes
only ``$null``: the renderer has no rule to bind a value to it."""


def _python_type_of(t: PostgresType) -> Any:
    scalar = _PYTHON_TYPE_OF[t.base]

    return GenericAlias(list, (scalar,)) if t.is_array else scalar


def _refuse_untyped(expr: QueryExpr, untyped: Mapping[str, str]) -> None:
    """Refuse any operator but ``$null`` on an *untyped* extra column (name to type)."""

    def _refuse(op: str, name: str) -> None:
        raise exc.precondition(
            f"Operator {op!r} is not valid for extra column {name!r}, a {untyped[name]} "
            "column: a metric filter takes only $null on a column of that type.",
            code=OPERATOR_TYPE_MISMATCH_CODE,
        )

    match expr:
        case QueryAnd(items) | QueryOr(items):
            for item in items:
                _refuse_untyped(item, untyped)

        case QueryNot(item):
            _refuse_untyped(item, untyped)

        case QueryField(name, op, _) if name in untyped and op != "$null":
            _refuse(op, name)

        case QueryCompare(left, op, right) if left in untyped or right in untyped:
            _refuse(op, left if left in untyped else right)

        case _:
            pass


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchAdapter[M: BaseModel](  # pyright: ignore[reportIncompatibleVariableOverride]
    PostgresGateway[M],
    HubSearchCursorMixin[M],
    PostgresSearchPortMixin[M],
    SearchQueryPort[M],
):
    """Search over a hub row type with one or more legs and merged per-leg scores."""

    hub_spec: HubSearchSpec[M]
    members: Sequence[HubLegRuntime]
    vector_embedders: Mapping[int, EmbeddingsProviderPort] = attrs.field(
        factory=dict[int, EmbeddingsProviderPort],
    )
    result_snapshot: SearchResultSnapshot | None = None
    combine: Literal["or", "and"] = "or"
    score_merge: Literal["max", "sum"] = "max"
    per_leg_limit: int = 5000
    """Max ranked rows retained per hub leg before merge."""

    combo_limit: int | None = None
    """Cap merged hub rows before outer pagination; ``None`` derives from :attr:`per_leg_limit`."""

    execution: Literal["sql", "parallel"] = "sql"
    """``sql``: one ``WITH`` query; ``parallel``: per-leg queries merged in Python."""

    parallel_hub_cte_materialized: bool = True
    """When ``execution=parallel``, use ``MATERIALIZED`` on the hub filter CTE per leg statement."""

    read_validation: Literal["strict", "trusted"] = "strict"
    """Row decode mode for hub search hits (``trusted`` skips Pydantic validation)."""

    spec: HubSearchSpec[M] = attrs.field(
        default=attrs.Factory(lambda self: self.hub_spec, takes_self=True),
        init=False,
    )
    """:attr:`hub_spec`, under the name the shared search port reads (aggregate checks)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.per_leg_limit < 1:
            raise exc.internal("per_leg_limit must be at least 1.")

        if self.combo_limit is not None and self.combo_limit < 1:
            raise exc.internal("combo_limit must be at least 1.")

        if self.execution not in ("sql", "parallel"):
            raise exc.internal("execution must be 'sql' or 'parallel'.")

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # Single-store hybrid: the hub's rank-based leg merge (score_merge) is the ``rrf``
        # fusion family; weighted relative-score fusion is a federated concept and is refused
        # rather than silently treated as the default merge.
        #
        # No ``supports_stream``: the hub fetches only the top ``per_leg_limit`` ranked rows
        # from each leg before merging, so a keyset walk (and a stream export) cannot go past
        # that per-leg bound — completeness is not guaranteed. Streaming fails closed; a hub
        # export uses a result snapshot instead.
        #
        # ``supports_aggregates``: an aggregate merges uncapped legs, the set an exact page
        # total counts, not the capped one a page ranks.
        return SearchCapabilities(hybrid_fusion=frozenset({"rrf"}), supports_aggregates=True)

    # ....................... #

    async def _aggregate_search_impl(
        self,
        aggregates: AggregatesExpression,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        *,
        options: SearchOptions | None,
        return_count: bool,
    ) -> CountlessPage[JsonDict] | Page[JsonDict]:
        """Group the merged hub rows an exact page total counts: every leg uncapped, no
        merge cap, the hub's filters applied. The rows carry the read fields and
        :attr:`~HubSearchSpec.extra_aggregatable_fields`."""

        resolve_fusion(
            cast("MultiSourceSearchOptions", options or {}).get("fusion"),
            self.search_capabilities,
            backend="postgres_hub",
        )
        if reserved := self.hub_spec.extra_aggregatable_fields & HUB_INTERNAL_ROW_KEYS:
            # The merged rows carry these as the pipeline's own columns; a hub column of the
            # same name would be ambiguous there.
            raise exc.configuration(
                f"Hub search {self.hub_spec.name!r}: extra aggregatable field(s) "
                f"{sorted(reserved)} use names the hub search reserves.",
            )

        types = await self.column_types()
        hub_qn = await self._qname()

        if missing := self.hub_spec.extra_aggregatable_fields - frozenset(types):
            raise exc.internal(
                f"Hub search {self.hub_spec.name!r} ({hub_qn.schema}.{hub_qn.name}): extra "
                f"aggregatable field(s) {sorted(missing)} are not columns of the hub relation.",
                code="postgres_schema_validation_failed",
                details={"missing": sorted(missing)},
            )

        # No read-model field types an extra column, so a metric filter's operators on one
        # are checked against the column's own type, as a filter's are against the read model.
        if extras := self.hub_spec.extra_aggregatable_fields:
            hints = {
                n: _python_type_of(types[n]) for n in extras if types[n].base in _PYTHON_TYPE_OF
            }
            untyped = {n: types[n].base for n in extras if types[n].base not in _PYTHON_TYPE_OF}

            for raw in collect_aggregate_filter_expressions(aggregates, parser=self.filter_parser):
                expr = self.filter_parser.parse_filter(raw)
                # ``BaseModel`` has no fields: the read-model ones are typed by the renderer,
                # as on every aggregate.
                validate_query_field_types(expr, BaseModel, field_type_hints=hints)
                _refuse_untyped(expr, untyped)

        # The cursor plan has no merge cap; ``uncapped_legs`` lifts the per-leg one.
        plan = await build_hub_search_plan(
            self,
            query=query,
            options=options,
            sorts=None,
            pagination_or_cursor={},
            snapshot=None,
            result_snapshot=None,
            mode="cursor",
        )

        if plan.do_legs and any(leg.engine == "vector" for _i, leg, _w in plan.active):
            # Uncapped, a vector leg matches every embedded row: there is no matched set.
            raise exc.precondition(
                f"Hub search {self.hub_spec.name!r}: an aggregate cannot measure a vector "
                "leg's matches; weight its member out with member_weights.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        with_clause, params, _, combo, _ = await self._hub_build_with_clause_from_plan(
            plan,
            filters=filters,
            uncapped_legs=True,
            fields=sorted(self.read_fields | self.hub_spec.extra_aggregatable_fields),
        )
        renderer = PsycopgQueryRenderer(
            types=types,
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
            table_alias=COMBO_ALIAS,
        )
        aggregate = compose_aggregate_statement(
            renderer,
            aggregates,
            filter_parser=self.filter_parser,
            source=sql.SQL("FROM {} AS {}").format(
                sql.Identifier(combo), sql.Identifier(COMBO_ALIAS)
            ),
            source_params=[],
        )

        return await execute_search_aggregate(
            self.client,
            head=with_clause,
            head_params=params,
            aggregate=aggregate,
            aggregates=aggregates,
            pagination=pagination,
            sorts=sorts,
            return_count=return_count,
        )

    # ....................... #

    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool = False,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        resolve_fusion(
            cast("MultiSourceSearchOptions", options or {}).get("fusion"),
            self.search_capabilities,
            backend="postgres_hub",
        )
        plan = await build_hub_search_plan(
            self,
            query=query,
            options=options,
            sorts=sorts,
            pagination_or_cursor=dict(pagination or {}),
            snapshot=snapshot,
            result_snapshot=self.result_snapshot,
            mode="offset",
        )

        if plan.use_parallel:
            # Parallel execution merges per-leg results in Python; facets reuse the ``sql``
            # companion GROUP BY (identical distribution across modes), highlights are marked
            # on the returned hits.
            facet_fields = resolve_facet_fields(self.hub_spec, options)
            parallel_page = await self._hub_parallel_offset_search(
                plan=plan,
                query=query,
                filters=filters,
                pagination=pagination,
                sorts=sorts,
                options=plan.leg_options,
                snapshot=snapshot,
                return_count=return_count,
                return_type=return_type,
                return_fields=return_fields,
                hub_spec=self.hub_spec,
                result_snapshot=self.result_snapshot,
            )
            if facet_fields:
                facets = await self._hub_parallel_facets(
                    plan,
                    filters=filters,
                    combo_limit=plan.resolved_combo if plan.do_legs else None,
                    fields=facet_fields,
                    size=facet_size_of(options),
                )
                parallel_page = attrs.evolve(parallel_page, facets=facets)
            return attach_hub_highlights(
                parallel_page,
                hub_spec=self.hub_spec,
                query=query,
                options=options,
                return_fields=return_fields,
            )

        # ``sql`` execution: facets via a companion GROUP BY over the merged set; highlights
        # are marked on the returned page below (the field validation runs in both helpers).
        facet_fields = resolve_facet_fields(self.hub_spec, options)

        combo_cap = plan.resolved_combo if plan.do_legs else None

        # Late materialization: project only key/sort columns through the WITH pipeline and
        # hydrate the heavy read-model columns for the page by id. Enabled whenever the shape
        # can be thinned safely; snapshot-writing requests hydrate per streamed window
        # (handled in the executor), so they no longer force the full projection.
        thin = self._hub_thin_projection(plan) is not None

        # do_legs (_) is not used for some reason
        (
            with_clause,
            params,
            _,
            count_relation,
            data_relation,
        ) = await self._hub_build_with_clause_from_plan(
            plan,
            filters=filters,
            combo_limit=combo_cap,
            thin=thin,
        )

        order_sql = await self.render_hub_order_sql(plan)

        approximate_total: int | None = None

        # Only a blank query's total is the planner's estimate of the filters; with search
        # text the executor counts the capped merged rows the page reads.
        if return_count and plan.count_policy == "approximate" and not plan.do_legs:
            fw, fp = await self.where_clause(filters)
            hub_qn = await self._qname()
            approximate_total = await resolve_ranked_approximate_total(
                introspector=self.introspector,
                schema=hub_qn.schema,
                relation=hub_qn.name,
                where_sql=fw,
                params=fp,
                alias=self.filter_table_alias,
            )

        # Surface the merged hub score (``_hub_rank``) as ``_score`` on ranked queries; a
        # filter-only browse (``not do_legs``) has no meaningful score.
        rank_select = (
            sql.SQL(", {}.{} AS {}").format(
                sql.Identifier(COMBO_ALIAS),
                sql.Identifier(HUB_RANK),
                sql.Identifier(SEARCH_SCORE_ALIAS),
            )
            if plan.do_legs
            else None
        )

        ranked_plan = RankedOffsetPlan(
            with_clause=with_clause,
            from_outer=sql.SQL(""),
            order_sql=order_sql,
            params=params,
            approximate_total=approximate_total,
            count_relation=count_relation,
            data_relation=data_relation,
            thin=thin,
            select_table_alias=COMBO_ALIAS,
            rank_select=rank_select,
        )

        members_weighted = hub_members_weighted(
            self.hub_spec,
            plan.member_weights_list,
        )

        page = await execute_hub_ranked_offset_search(
            self,
            plan=ranked_plan,
            query=query,
            filters=filters,
            sorts=sorts,
            hub_spec=self.hub_spec,
            members_weighted=members_weighted,
            score_merge=str(self.score_merge),
            combine=str(self.combine),
            per_leg_limit=self.per_leg_limit,
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            result_snapshot=self.result_snapshot,
            combo_alias=COMBO_ALIAS,
            options=plan.leg_options,
            execution=str(self.execution),
            combo_limit=combo_cap,
            trust_source=search_trust_source(self.read_validation),
            facet_fields=facet_fields,
            facet_size=facet_size_of(options),
        )

        return attach_hub_highlights(
            page,
            hub_spec=self.hub_spec,
            query=query,
            options=options,
            return_fields=return_fields,
        )
