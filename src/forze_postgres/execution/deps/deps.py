"""Factory functions for Postgres document and tx manager adapters."""

from typing import Any, Literal, Sequence, cast, final

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.embeddings import (
    EmbeddingsProviderPort,
    EmbeddingsSpec,
)
from forze.application.contracts.search import (
    FederatedSearchQueryDepPort,
    FederatedSearchSpec,
    HubSearchQueryDepPort,
    HubSearchSpec,
    SearchQueryDepPort,
    SearchQueryPort,
    SearchSpec,
)
from forze.application.contracts.tx import TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import ID_FIELD

from ...adapters import (
    FtsGroupLetter,
    HubLegRuntime,
    PostgresDocumentAdapter,
    PostgresFederatedSearchAdapter,
    PostgresFTSSearchAdapterV2,
    PostgresHubSearchAdapter,
    PostgresPGroongaSearchAdapterV2,
    PostgresTxManagerAdapter,
    PostgresVectorSearchAdapterV2,
)
from ...kernel.gateways import PostgresQualifiedName
from .._logger import logger
from .configs import (
    PostgresDocumentConfig,
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    VectorEngineDistance,
    is_postgres_federated_embedded_hub_config,
    validate_fts_groups_for_search_spec,
    validate_pg_search_conf,
    validate_postgres_federated_search_conf,
    validate_postgres_hub_search_conf,
)
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresReadOnlyDocument(DocumentQueryDepPort):
    """Configurable Postgres read-only document adapter."""

    config: PostgresReadOnlyDocumentConfig
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: CachePort | None = None,
    ) -> PostgresDocumentAdapter[Any, Any, Any, Any]:
        read = read_gw(
            context,
            read_type=spec.read,
            read_relation=self.config["read"],
            tenant_aware=self.config.get("tenant_aware", False),
            nested_field_hints=self.config.get("nested_field_hints"),
        )

        return PostgresDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            cache=cache,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresDocument(DocumentCommandDepPort):
    """Configurable Postgres document adapter."""

    config: PostgresDocumentConfig
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: CachePort | None = None,
    ) -> PostgresDocumentAdapter[Any, Any, Any, Any]:
        tenant_aware = self.config.get("tenant_aware", False)

        if spec.write is None:
            raise CoreError("Write relation is required for non read-only documents.")

        read = read_gw(
            context,
            read_type=spec.read,
            read_relation=self.config["read"],
            tenant_aware=tenant_aware,
            nested_field_hints=self.config.get("nested_field_hints"),
        )

        write_relation = self.config["write"]
        history_relation = self.config.get("history")
        bookkeeping_strategy = self.config["bookkeeping_strategy"]

        # We only log a warning here because skipping history gateway is not critical.
        if history_relation is None and spec.history_enabled:
            logger.warning(
                f"History relation not found for document '{spec.name}' but history is enabled. Skipping history gateway"
            )

        elif history_relation is not None and not spec.history_enabled:
            logger.warning(
                f"History relation found for document '{spec.name}' but history is disabled. Skipping history gateway"
            )

        write = doc_write_gw(
            context,
            write_types=spec.write,
            write_relation=write_relation,
            history_relation=history_relation,
            history_enabled=spec.history_enabled,
            bookkeeping_strategy=bookkeeping_strategy,
            tenant_aware=tenant_aware,
            nested_field_hints=self.config.get("nested_field_hints"),
        )

        return PostgresDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            cache=cache,
            batch_size=self.config.get("batch_size", 200),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresSearch(SearchQueryDepPort):
    """Configurable Postgres search adapter."""

    config: PostgresSearchConfig
    """Configurations for the search."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> (
        PostgresPGroongaSearchAdapterV2[Any]
        | PostgresFTSSearchAdapterV2[Any]
        | PostgresVectorSearchAdapterV2[Any]
    ):
        return _postgres_search_port_for_config(context, spec, self.config)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresHubSearch(HubSearchQueryDepPort):
    """Build :class:`PostgresHubSearchAdapter` from spec + :class:`PostgresHubSearchConfig`."""

    config: PostgresHubSearchConfig
    """Postgres hub relation, per-leg indexes/heaps, merge options."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: HubSearchSpec[Any],
    ) -> PostgresHubSearchAdapter[Any]:
        validate_postgres_hub_search_conf(self.config)

        hub = PostgresQualifiedName(*self.config["hub"])
        tenant_aware = self.config.get("tenant_aware", False)

        members: list[HubLegRuntime] = []
        vector_embedders: dict[int, EmbeddingsProviderPort] = {}

        for i, m in enumerate(spec.members):
            c = self.config["members"].get(m.name)

            if c is None:
                raise CoreError(
                    f"Member '{m.name}' not found in PostgresHubSearchConfig['members']."
                )

            engine = c.get("engine", "pgroonga")

            fts_groups: dict[FtsGroupLetter, Sequence[str]] | None = None
            v_col: str | None = None
            v_dim: int | None = None
            v_dist: VectorEngineDistance = c.get("vector_distance", "l2")

            if engine == "fts":
                fts_groups = c.get("fts_groups")

                if fts_groups is None:
                    raise CoreError("FTS groups are required for FTS hub leg.")

                validate_fts_groups_for_search_spec(m, fts_groups)

            elif engine == "vector":
                v_col = c.get("vector_column")
                v_dim = c.get("embedding_dimensions")
                e_name = c.get("embeddings_name")
                if v_col is None or v_dim is None or e_name is None:
                    raise CoreError(
                        "vector hub leg requires vector_column, embedding_dimensions, and embeddings_name.",
                    )
                vector_embedders[i] = context.embeddings_provider(
                    EmbeddingsSpec(
                        name=str(e_name),
                        dimensions=int(v_dim),
                    )
                )

            elif engine != "pgroonga":
                raise CoreError(
                    f"Hub search leg engine {engine!r} is not supported; "
                    "use 'pgroonga', 'fts', or 'vector'."
                )

            if c.get("same_heap_as_hub"):
                hub_fields = pydantic_field_names(spec.model_type)
                for field in m.fields:
                    if field not in hub_fields:
                        raise CoreError(
                            f"same_heap_as_hub member {m.name!r}: search field {field!r} must "
                            "be a field on the hub SearchSpec model_type (hub row shape).",
                        )

            pg_sv: Literal["v1", "v2"] | None = None
            if engine == "pgroonga":
                pg_sv = c.get("pgroonga_score_version", "v2")
                if pg_sv not in ("v1", "v2"):
                    raise CoreError("pgroonga_score_version must be 'v1' or 'v2'.")

            rt = HubLegRuntime(
                search=m,
                index_qname=PostgresQualifiedName(*c["index"]),
                index_heap_qname=PostgresQualifiedName(*c.get("heap", c["read"])),
                hub_fk_columns=c["hub_fk"],
                heap_pk_column=c.get("heap_pk", ID_FIELD),
                index_field_map=c.get("field_map"),
                pgroonga_score_version=pg_sv,
                engine=engine,
                fts_groups=fts_groups,
                vector_column=v_col,
                vector_distance=v_dist,
                embedding_dimensions=v_dim,
                same_heap_as_hub=bool(c.get("same_heap_as_hub", False)),
            )
            members.append(rt)

        return PostgresHubSearchAdapter(
            hub_spec=spec,
            members=members,
            vector_embedders=vector_embedders,
            combine=self.config.get("combine_strategy", "or"),
            score_merge=self.config.get("merge_strategy", "max"),
            source_qname=hub,
            client=context.dep(PostgresClientDepKey),
            model_type=spec.model_type,
            introspector=context.dep(PostgresIntrospectorDepKey),
            tenant_provider=context.get_tenant_id,
            tenant_aware=tenant_aware,
            filter_table_alias="h",
            nested_field_hints=self.config.get("nested_field_hints"),
        )


# ....................... #


def _postgres_search_port_for_config(
    context: ExecutionContext,
    member_spec: SearchSpec[Any],
    c: PostgresSearchConfig,
) -> (
    PostgresPGroongaSearchAdapterV2[Any]
    | PostgresFTSSearchAdapterV2[Any]
    | PostgresVectorSearchAdapterV2[Any]
):
    validate_pg_search_conf(c)

    tenant_aware = c.get("tenant_aware", False)
    index_qname = PostgresQualifiedName(*c["index"])
    read_qname = PostgresQualifiedName(*c["read"])
    heap_qname = PostgresQualifiedName(*c.get("heap", c["read"]))

    match c["engine"]:
        case "pgroonga":
            return PostgresPGroongaSearchAdapterV2(
                spec=member_spec,
                index_qname=index_qname,
                source_qname=read_qname,
                index_heap_qname=heap_qname,
                join_pairs=c.get("join_pairs"),
                index_field_map=c.get("field_map"),
                pgroonga_score_version=c.get("pgroonga_score_version", "v2"),
                client=context.dep(PostgresClientDepKey),
                model_type=member_spec.model_type,
                introspector=context.dep(PostgresIntrospectorDepKey),
                tenant_provider=context.get_tenant_id,
                tenant_aware=tenant_aware,
                filter_table_alias="v",
                nested_field_hints=c.get("nested_field_hints"),
            )

        case "fts":
            fts_groups = c.get("fts_groups")

            if fts_groups is None:
                raise CoreError("FTS groups are required for FTS engine.")

            validate_fts_groups_for_search_spec(member_spec, fts_groups)

            return PostgresFTSSearchAdapterV2(
                spec=member_spec,
                index_qname=index_qname,
                source_qname=read_qname,
                index_heap_qname=heap_qname,
                fts_groups=fts_groups,
                join_pairs=c.get("join_pairs"),
                index_field_map=c.get("field_map"),
                client=context.dep(PostgresClientDepKey),
                model_type=member_spec.model_type,
                introspector=context.dep(PostgresIntrospectorDepKey),
                tenant_provider=context.get_tenant_id,
                tenant_aware=tenant_aware,
                filter_table_alias="v",
                nested_field_hints=c.get("nested_field_hints"),
            )

        case "vector":
            en = c.get("embeddings_name")
            ed = c.get("embedding_dimensions")
            vcol = c.get("vector_column")
            if en is None or ed is None or vcol is None:
                raise CoreError(
                    "vector engine requires embeddings_name, embedding_dimensions, and vector_column.",
                )
            es = EmbeddingsSpec(
                name=str(en),
                dimensions=int(ed),
            )
            return PostgresVectorSearchAdapterV2(
                spec=member_spec,
                index_qname=index_qname,
                source_qname=read_qname,
                index_heap_qname=heap_qname,
                embedder=context.embeddings_provider(es),
                embeddings_spec=es,
                vector_column=str(vcol),
                vector_distance=c.get("vector_distance", "l2"),
                join_pairs=c.get("join_pairs"),
                index_field_map=c.get("field_map"),
                client=context.dep(PostgresClientDepKey),
                model_type=member_spec.model_type,
                introspector=context.dep(PostgresIntrospectorDepKey),
                tenant_provider=context.get_tenant_id,
                tenant_aware=tenant_aware,
                filter_table_alias="v",
                nested_field_hints=c.get("nested_field_hints"),
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresFederatedSearch(FederatedSearchQueryDepPort):
    """Build :class:`PostgresFederatedSearchAdapter` from spec + config."""

    config: PostgresFederatedSearchConfig
    """Per-member single-index search configuration."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: FederatedSearchSpec[Any],
    ) -> PostgresFederatedSearchAdapter[Any]:
        validate_postgres_federated_search_conf(self.config)

        legs: list[tuple[str, SearchQueryPort[Any]]] = []

        for m in spec.members:
            c = self.config["members"].get(m.name)

            if c is None:
                raise CoreError(
                    f"Member '{m.name}' not found in PostgresFederatedSearchConfig['members'].",
                )

            if isinstance(m, HubSearchSpec):
                if not is_postgres_federated_embedded_hub_config(c):
                    raise CoreError(
                        f"Federated hub member {m.name!r} must use a Postgres config with "
                        "'hub' and 'members' (embedded PostgresHubSearchConfig).",
                    )

                hub_cfg = dict(c)

                if "tenant_aware" not in hub_cfg:
                    hub_cfg["tenant_aware"] = self.config.get("tenant_aware", False)

                port = ConfigurablePostgresHubSearch(
                    config=cast(PostgresHubSearchConfig, hub_cfg),
                )(context, m)
                legs.append((m.name, port))
                continue

            engine = c.get("engine", "pgroonga")

            if is_postgres_federated_embedded_hub_config(c):
                raise CoreError(
                    f"Federated search member {m.name!r} is a SearchSpec but its Postgres "
                    "config looks like an embedded hub (has 'hub' and 'members').",
                )

            if engine not in ("pgroonga", "fts", "vector"):
                raise CoreError(
                    f"Federated search member engine {engine!r} is not supported; "
                    "use 'pgroonga', 'fts', or 'vector'.",
                )

            if engine == "fts":
                fts_groups = c.get("fts_groups")

                if fts_groups is None:
                    raise CoreError("FTS groups are required for FTS federated member.")

                validate_fts_groups_for_search_spec(m, fts_groups)  # type: ignore[arg-type]

            port_plain = _postgres_search_port_for_config(
                context,
                m,
                cast(PostgresSearchConfig, c),
            )
            legs.append((m.name, port_plain))

        return PostgresFederatedSearchAdapter(
            federated_spec=spec,
            legs=tuple(legs),
            rrf_k=int(self.config.get("rrf_k", 60)),
            rrf_per_leg_limit=int(self.config.get("rrf_per_leg_limit", 5000)),
        )


# ....................... #


#! convert to a simple class maybe
def postgres_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Postgres-backed transaction manager for the execution context.

    :param context: Execution context for resolving the Postgres client.
    :returns: Tx manager port backed by :class:`PostgresTxManagerAdapter`.
    """

    client = context.dep(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)
