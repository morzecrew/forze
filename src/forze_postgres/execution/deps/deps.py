"""Factory functions for Postgres document and tx manager adapters."""

from functools import reduce
from typing import Any, Sequence

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tx import TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError

from ...adapters import (
    FtsGroupLetter,
    PostgresDocumentAdapter,
    PostgresFTSSearchAdapter,
    PostgresPGroongaSearchAdapter,
    PostgresTxManagerAdapter,
)
from ...kernel.gateways import PostgresBookkeepingStrategy, PostgresQualifiedName
from .._logger import logger
from .configs import PostgresDocumentConfigs, PostgresSearchConfigs
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresDocument:
    """Configurable Postgres document adapter."""

    bookkeeping_strategy: PostgresBookkeepingStrategy
    """Bookkeeping strategy."""

    configs: PostgresDocumentConfigs
    """Configurations for the document."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: CachePort | None = None,
    ) -> PostgresDocumentAdapter[Any, Any, Any, Any]:
        if spec.name not in self.configs.keys():
            raise CoreError(f"No configuration found for document '{spec.name}'")

        config = self.configs[spec.name]
        tenant_aware = config.get("tenant_aware", False)

        read = read_gw(
            context,
            read_type=spec.read,
            read_relation=config["read"],
            tenant_aware=tenant_aware,
        )
        write = None

        write_relation = config.get("write")
        history_relation = config.get("history")

        if spec.write is not None:
            if write_relation is None:
                # We raise an error here because skipping write gateway would break application logic.
                raise CoreError(f"No write relation found for document '{spec.name}'")

            else:
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
                    bookkeeping_strategy=self.bookkeeping_strategy,
                    tenant_aware=tenant_aware,
                )

        return PostgresDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            cache=cache,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresSearch:
    """Configurable Postgres search adapter."""

    configs: PostgresSearchConfigs
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
    ) -> PostgresPGroongaSearchAdapter[Any] | PostgresFTSSearchAdapter[Any]:
        if spec.name not in self.configs.keys():
            raise CoreError(f"No configuration found for search '{spec.name}'")

        config = self.configs[spec.name]
        tenant_aware = config.get("tenant_aware", False)

        qname = PostgresQualifiedName(
            schema=config["index"][0],
            name=config["index"][1],
        )
        source_qname = PostgresQualifiedName(
            schema=config["source"][0],
            name=config["source"][1],
        )

        match config["engine"]:
            case "pgroonga":
                return PostgresPGroongaSearchAdapter(
                    spec=spec,
                    qname=qname,
                    source_qname=source_qname,
                    client=context.dep(PostgresClientDepKey),
                    model_type=spec.model_type,
                    introspector=context.dep(PostgresIntrospectorDepKey),
                    tenant_provider=context.get_tenant_id,
                    tenant_aware=tenant_aware,
                )

            case "fts":
                fts_groups = config.get("fts_groups")

                if fts_groups is None:
                    raise CoreError("FTS groups are required for FTS engine.")

                self.__validate_fts_groups(spec, fts_groups)

                return PostgresFTSSearchAdapter(
                    spec=spec,
                    qname=qname,
                    source_qname=source_qname,
                    client=context.dep(PostgresClientDepKey),
                    model_type=spec.model_type,
                    introspector=context.dep(PostgresIntrospectorDepKey),
                    tenant_provider=context.get_tenant_id,
                    tenant_aware=tenant_aware,
                    fts_groups=fts_groups,
                )


# ....................... #

#! Need to set transaction options on usecase level rather than here.


def postgres_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Postgres-backed transaction manager for the execution context.

    :param context: Execution context for resolving the Postgres client.
    :returns: Tx manager port backed by :class:`PostgresTxManagerAdapter`.
    """

    client = context.dep(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)
