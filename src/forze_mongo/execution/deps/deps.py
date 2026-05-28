"""Factory functions for Mongo document and tx manager adapters."""

from typing import Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import (
    SearchQueryDepPort,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.contracts.search.ports import SearchQueryPort
from forze.application.contracts.transaction import (
    AfterCommitPort,
    TransactionManagerPort,
)
from forze.application.coordinators import (
    DocumentCacheCoordinator,
    SearchResultSnapshotCoordinator,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ...adapters import (
    MongoAtlasSearchAdapter,
    MongoDocumentAdapter,
    MongoTextSearchAdapter,
    MongoTxManagerAdapter,
    MongoVectorSearchAdapter,
)
from .._logger import logger
from .configs import (
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
)
from .keys import MongoClientDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoReadOnlyDocument(DocumentQueryDepPort[R]):
    """Configurable Mongo read-only document adapter."""

    config: MongoReadOnlyDocumentConfig
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, Any, Any, Any],
    ) -> MongoDocumentAdapter[R, Any, Any, Any]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config.read,
            tenant_aware=self.config.tenant_aware,
        )

        after_commit: AfterCommitPort | None = None

        if cache is not None:
            after_commit = ctx.tx_ctx.run_or_defer

        cc = DocumentCacheCoordinator[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return MongoDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            cache_coord=cc,
            batch_size=self.config.batch_size,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoDocument(DocumentCommandDepPort[R, D, C, U]):
    """Configurable Mongo document adapter."""

    config: MongoDocumentConfig
    """Configurations for the document."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, D, C, U],
    ) -> MongoDocumentAdapter[R, D, C, U]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None
        config = self.config
        tenant_aware = config.tenant_aware

        if spec.write is None:
            raise exc.internal(
                "Write relation is required for non read-only documents."
            )

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=config.read,
            tenant_aware=tenant_aware,
        )

        write_relation = config.write
        history_relation = config.history

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
            ctx,
            write_types=spec.write,
            write_relation=write_relation,
            history_relation=history_relation,
            history_enabled=spec.history_enabled,
            tenant_aware=tenant_aware,
        )

        after_commit: AfterCommitPort | None = None

        if cache is not None:
            after_commit = ctx.tx_ctx.run_or_defer

        cc = DocumentCacheCoordinator[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return MongoDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            cache_coord=cc,
            batch_size=config.batch_size,
        )


# ....................... #


def _resolve_result_snapshot(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
) -> Any:
    if spec is None:
        return None

    if not (
        context.deps.exists(SearchResultSnapshotDepKey, route=spec.name)
        or context.deps.exists(SearchResultSnapshotDepKey)
    ):
        return None

    return context.deps.provide(SearchResultSnapshotDepKey, route=spec.name)(
        context,
        spec,
    )


# ....................... #


def _snapshot_coord(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
) -> SearchResultSnapshotCoordinator | None:
    port = _resolve_result_snapshot(context, spec)

    if port is None:
        return None

    return SearchResultSnapshotCoordinator(store=port)


# ....................... #


def _mongo_search_port_for_config(
    context: ExecutionContext,
    member_spec: SearchSpec[Any],
    c: MongoSearchConfig,
) -> (
    MongoTextSearchAdapter[Any]
    | MongoAtlasSearchAdapter[Any]
    | MongoVectorSearchAdapter[Any]
):
    c.validate_against_spec(member_spec)

    db_name, coll_name = c.read
    field_map = dict(c.field_map or {})
    snapshot_coord = _snapshot_coord(context, member_spec.snapshot)
    client = context.deps.provide(MongoClientDepKey)
    tenant_aware = c.tenant_aware

    match c.engine:
        case "text":
            return MongoTextSearchAdapter(
                spec=member_spec,
                model_type=member_spec.model_type,
                database=db_name,
                collection=coll_name,
                client=client,
                field_map=field_map,
                tenant_provider=context.inv_ctx.get_tenant,
                tenant_aware=tenant_aware,
                snapshot_coord=snapshot_coord,
            )

        case "atlas":
            index_name = c.index_name

            return MongoAtlasSearchAdapter(
                spec=member_spec,
                model_type=member_spec.model_type,
                database=db_name,
                collection=coll_name,
                client=client,
                field_map=field_map,
                tenant_provider=context.inv_ctx.get_tenant,
                tenant_aware=tenant_aware,
                snapshot_coord=snapshot_coord,
                index_name=str(index_name),
            )

        case "vector":
            en = c.embeddings_name
            ed = c.embedding_dimensions
            vpath = c.vector_path
            index_name = c.index_name

            if en is None or ed is None or vpath is None or index_name is None:
                raise exc.internal(
                    "vector engine requires embeddings_name, embedding_dimensions, "
                    "vector_path, and index_name.",
                )

            es = EmbeddingsSpec(name=en, dimensions=ed)

            return MongoVectorSearchAdapter(
                spec=member_spec,
                model_type=member_spec.model_type,
                database=db_name,
                collection=coll_name,
                client=client,
                field_map=field_map,
                tenant_provider=context.inv_ctx.get_tenant,
                tenant_aware=tenant_aware,
                snapshot_coord=snapshot_coord,
                embedder=context.embeddings.provider(es),
                embedding_dimensions=ed,
                vector_path=vpath,
                index_name=index_name,
            )

        case _:  # pyright: ignore[reportUnnecessaryComparison]
            raise exc.internal(f"Unsupported Mongo search engine: {c.engine!r}.")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoSearch(SearchQueryDepPort):
    """Configurable Mongo search adapter factory."""

    config: MongoSearchConfig
    """Mongo-specific search configuration."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> SearchQueryPort[Any]:
        return _mongo_search_port_for_config(context, spec, self.config)


# ....................... #


#! convert to a simple class maybe
def mongo_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""

    client = context.deps.provide(MongoClientDepKey)

    return MongoTxManagerAdapter(client=client)
