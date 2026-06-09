"""Build :class:`HubLegRuntime` lists from hub search specs and Postgres config."""

from typing import TYPE_CHECKING, Any, Literal

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import HubSearchSpec
from forze.base.exceptions import exc
from forze.application.contracts.codecs import stored_field_names_for

from ....adapters import HubLegRuntime
from ..configs import PostgresHubSearchConfig
from ..configs.search import validate_fts_groups_for_search_spec

if TYPE_CHECKING:
    from forze.application.contracts.embeddings import EmbeddingsProviderPort
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def build_hub_leg_runtimes(
    context: "ExecutionContext",
    spec: HubSearchSpec[Any],
    config: PostgresHubSearchConfig,
) -> tuple[list["HubLegRuntime"], dict[int, "EmbeddingsProviderPort"]]:
    """Resolve hub member legs from spec members and per-member Postgres config."""

    members: list[HubLegRuntime] = []
    vector_embedders: dict[int, "EmbeddingsProviderPort"] = {}

    for i, m in enumerate(spec.members):
        c = config.members.get(m.name)

        if c is None:
            raise exc.internal(
                f"Member '{m.name}' not found in PostgresHubSearchConfig.members."
            )

        engine = c.engine

        fts_groups = c.fts_groups
        v_col = c.vector_column
        v_dim = c.embedding_dimensions
        v_dist = c.vector_distance

        if engine == "fts":
            if fts_groups is None:
                raise exc.internal("FTS groups are required for FTS hub leg.")

            validate_fts_groups_for_search_spec(m, fts_groups)

        elif engine == "vector":
            e_name = c.embeddings_name

            if v_col is None or v_dim is None or e_name is None:
                raise exc.internal(
                    "vector hub leg requires vector_column, embedding_dimensions, and embeddings_name.",
                )

            vector_embedders[i] = context.embeddings.provider(
                EmbeddingsSpec(
                    name=str(e_name),
                    dimensions=int(v_dim),
                )
            )

        elif engine != "pgroonga":
            raise exc.internal(
                f"Hub search leg engine {engine!r} is not supported; "
                "use 'pgroonga', 'fts', or 'vector'."
            )

        if c.same_heap_as_hub:
            hub_fields = stored_field_names_for(
                spec.model_type,
                include_computed=False,
            )

            for field in m.fields:
                if field not in hub_fields:
                    raise exc.internal(
                        f"same_heap_as_hub member {m.name!r}: search field {field!r} must "
                        "be a field on the hub SearchSpec model_type (hub row shape).",
                    )

        pg_sv: Literal["v1", "v2"] | None = None

        if engine == "pgroonga":
            pg_sv = c.pgroonga_score_version

        rt = HubLegRuntime(
            search=m,
            index_relation=c.index,
            index_heap_relation=c.heap_relation,
            hub_fk_columns=c.hub_fk,
            heap_pk_column=c.heap_pk,
            index_field_map=c.field_map,
            pgroonga_score_version=pg_sv,
            engine=engine,
            fts_groups=fts_groups,
            vector_column=v_col,
            vector_distance=v_dist,
            embedding_dimensions=v_dim,
            same_heap_as_hub=c.same_heap_as_hub,
        )
        members.append(rt)

    return members, vector_embedders
