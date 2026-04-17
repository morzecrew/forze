"""Factory functions for Postgres document and tx manager adapters."""

from functools import reduce
from typing import Any, Sequence, final

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.search import (
    HubSearchQueryDepPort,
    HubSearchSpec,
    SearchQueryDepPort,
    SearchSpec,
)
from forze.application.contracts.tx import TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.domain.constants import ID_FIELD

from ...adapters import (
    FtsGroupLetter,
    HubLegRuntime,
    PostgresDocumentAdapter,
    PostgresFTSSearchAdapter,
    PostgresHubPGroongaSearchAdapter,
    PostgresPGroongaSearchAdapterV2,
    PostgresTxManagerAdapter,
)
from ...kernel.gateways import PostgresQualifiedName
from .._logger import logger
from .configs import (
    PostgresDocumentConfig,
    PostgresHubSearchConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
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

    def __validate_fts_groups(
        self,
        spec: SearchSpec[Any],
        fts_groups: dict[FtsGroupLetter, Sequence[str]],
    ) -> None:
        """Validate FTS groups."""

        if not fts_groups:
            raise CoreError("FTS groups are required for FTS engine.")

        grouped_fields = reduce(lambda a, g: a + g, map(list, fts_groups.values()))

        if any(f not in grouped_fields for f in spec.fields):
            raise CoreError("All search fields must be included in FTS groups.")

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> PostgresPGroongaSearchAdapterV2[Any] | PostgresFTSSearchAdapter[Any]:
        tenant_aware = self.config.get("tenant_aware", False)

        index_qname = PostgresQualifiedName(*self.config["index"])
        read_qname = PostgresQualifiedName(*self.config["read"])
        heap_qname = PostgresQualifiedName(
            *self.config.get("heap", self.config["read"])
        )

        match self.config["engine"]:
            case "pgroonga":
                return PostgresPGroongaSearchAdapterV2(
                    spec=spec,
                    index_qname=index_qname,
                    source_qname=read_qname,
                    index_heap_qname=heap_qname,
                    join_pairs=self.config.get("join_pairs"),
                    index_field_map=self.config.get("field_map"),
                    client=context.dep(PostgresClientDepKey),
                    model_type=spec.model_type,
                    introspector=context.dep(PostgresIntrospectorDepKey),
                    tenant_provider=context.get_tenant_id,
                    tenant_aware=tenant_aware,
                )

            case "fts":
                fts_groups = self.config.get("fts_groups")

                if fts_groups is None:
                    raise CoreError("FTS groups are required for FTS engine.")

                self.__validate_fts_groups(spec, fts_groups)

                return PostgresFTSSearchAdapter(
                    spec=spec,
                    source_qname=read_qname,
                    index_qname=index_qname,
                    client=context.dep(PostgresClientDepKey),
                    model_type=spec.model_type,
                    introspector=context.dep(PostgresIntrospectorDepKey),
                    tenant_provider=context.get_tenant_id,
                    tenant_aware=tenant_aware,
                    fts_groups=fts_groups,
                )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresHubSearch(HubSearchQueryDepPort):
    """Build :class:`PostgresHubPGroongaSearchAdapter` from spec + :class:`PostgresHubSearchConfig`."""

    config: PostgresHubSearchConfig
    """Postgres hub relation, per-leg indexes/heaps, merge options."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: HubSearchSpec[Any],
    ) -> PostgresHubPGroongaSearchAdapter[Any]:
        validate_postgres_hub_search_conf(self.config)

        if len(spec.members) != len(self.config["members"]):
            raise CoreError(
                "HubSearchSpec.members and PostgresHubSearchConfig['members'] must have the same length."
            )

        hub = PostgresQualifiedName(*self.config["hub"])
        tenant_aware = self.config.get("tenant_aware", False)

        members: list[HubLegRuntime] = []

        for m in spec.members:
            c = self.config["members"].get(m.name)

            if c is None:
                raise CoreError(
                    f"Member '{m.name}' not found in PostgresHubSearchConfig['members']."
                )

            rt = HubLegRuntime(
                search=m,
                index_qname=PostgresQualifiedName(*c["index"]),
                index_heap_qname=PostgresQualifiedName(*c.get("heap", c["read"])),
                hub_fk_column=c["hub_fk"],
                heap_pk_column=c.get("heap_pk", ID_FIELD),
                index_field_map=c.get("field_map"),
            )
            members.append(rt)

        return PostgresHubPGroongaSearchAdapter(
            hub_spec=spec,
            members=members,
            combine=self.config.get("combine_strategy", "or"),
            score_merge=self.config.get("merge_strategy", "max"),
            source_qname=hub,
            client=context.dep(PostgresClientDepKey),
            model_type=spec.model_type,
            introspector=context.dep(PostgresIntrospectorDepKey),
            tenant_provider=context.get_tenant_id,
            tenant_aware=tenant_aware,
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
