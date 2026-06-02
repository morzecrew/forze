"""Postgres single-index search dep factories."""

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import SearchQueryDepPort
from forze.base.exceptions import exc

from ....adapters import (
    PostgresFTSSearchAdapter,
    PostgresPGroongaSearchAdapter,
    PostgresVectorSearchAdapter,
)
from ..configs import PostgresSearchConfig, validate_fts_groups_for_search_spec
from ..keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from ._snapshot import result_snapshot

if TYPE_CHECKING:
    from forze.application.contracts.search import SearchSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresSearch(SearchQueryDepPort):
    """Configurable Postgres search adapter."""

    config: PostgresSearchConfig
    """Configurations for the search."""

    # ....................... #

    def __call__(
        self,
        context: "ExecutionContext",
        spec: "SearchSpec[Any]",
    ) -> (
        PostgresPGroongaSearchAdapter[Any]
        | PostgresFTSSearchAdapter[Any]
        | PostgresVectorSearchAdapter[Any]
    ):
        return postgres_search_port_for_config(context, spec, self.config)


# ....................... #


def postgres_search_port_for_config(
    context: "ExecutionContext",
    member_spec: "SearchSpec[Any]",
    c: PostgresSearchConfig,
) -> (
    PostgresPGroongaSearchAdapter[Any]
    | PostgresFTSSearchAdapter[Any]
    | PostgresVectorSearchAdapter[Any]
):
    snap = result_snapshot(context, member_spec.snapshot)

    common = dict(
        spec=member_spec,
        codec=member_spec.resolved_read_codec,
        relation=c.read,
        index_relation=c.index,
        index_heap_relation=c.heap_relation,
        join_pairs=c.join_pairs,
        index_field_map=c.field_map,
        client=context.deps.provide(PostgresClientDepKey),
        model_type=member_spec.model_type,
        introspector=context.deps.provide(PostgresIntrospectorDepKey),
        tenant_provider=context.inv_ctx.get_tenant,
        tenant_aware=c.tenant_aware,
        filter_table_alias="v",
        nested_field_hints=c.nested_field_hints,
        result_snapshot=snap,
    )

    match c.engine:
        case "pgroonga":
            return PostgresPGroongaSearchAdapter[Any](
                **common,  # type: ignore[arg-type]
                pgroonga_score_version=c.pgroonga_score_version,
                pgroonga_plan=c.pgroonga_plan,
                pgroonga_candidate_limit=c.pgroonga_candidate_limit,
                pgroonga_auto_index_first_min_rows=c.pgroonga_auto_index_first_min_rows,
                pgroonga_auto_use_exact_count=c.pgroonga_auto_use_exact_count,
                pgroonga_auto_with_filters=c.pgroonga_auto_with_filters,
                pgroonga_auto_filter_first_max_rows=c.pgroonga_auto_filter_first_max_rows,
                pgroonga_index_first_filter_margin=c.pgroonga_index_first_filter_margin,
                read_relation=c.read,
                heap_relation_spec=c.heap_relation,
            )

        case "fts":
            fts_groups = c.fts_groups

            if fts_groups is None:
                raise exc.internal("FTS groups are required for FTS engine.")

            validate_fts_groups_for_search_spec(member_spec, fts_groups)

            return PostgresFTSSearchAdapter[Any](
                **common,  # type: ignore[arg-type]
                fts_groups=fts_groups,
                ranked_candidate_limit=c.pgroonga_candidate_limit,
                read_relation=c.read,
                heap_relation_spec=c.heap_relation,
            )

        case "vector":
            en = c.embeddings_name
            ed = c.embedding_dimensions
            vcol = c.vector_column

            if en is None or ed is None or vcol is None:
                raise exc.internal(
                    "vector engine requires embeddings_name, embedding_dimensions, and vector_column.",
                )

            es = EmbeddingsSpec(
                name=str(en),
                dimensions=int(ed),
            )

            return PostgresVectorSearchAdapter[Any](
                **common,  # type: ignore[arg-type]
                embedder=context.embeddings.provider(es),
                embeddings_spec=es,
                vector_column=str(vcol),
                vector_distance=c.vector_distance,
                ranked_candidate_limit=c.pgroonga_candidate_limit,
                read_relation=c.read,
                heap_relation_spec=c.heap_relation,
            )
