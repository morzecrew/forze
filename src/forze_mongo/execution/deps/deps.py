"""Factory functions for Mongo document and tx manager adapters."""

from typing import Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.tx import AfterCommitPort, TxManagerPort
from forze.application.coordinators import DocumentCacheCoordinator
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ...adapters import MongoDocumentAdapter, MongoTxManagerAdapter
from .._logger import logger
from .configs import MongoDocumentConfig, MongoReadOnlyDocumentConfig
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
            read_relation=self.config["read"],
            tenant_aware=self.config.get("tenant_aware", False),
        )

        after_commit: AfterCommitPort | None = None

        if cache is not None:
            after_commit = ctx.run_after_commit_or_now

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
            batch_size=self.config.get("batch_size", 200),
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
        tenant_aware = config.get("tenant_aware", False)

        if spec.write is None:
            raise CoreError("Write relation is required for non read-only documents.")

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=config["read"],
            tenant_aware=tenant_aware,
        )

        write_relation = config["write"]
        history_relation = config.get("history")

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
            after_commit = ctx.run_after_commit_or_now

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
            batch_size=config.get("batch_size", 200),
        )


# ....................... #


#! convert to a simple class maybe
def mongo_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""

    client = context.dep(MongoClientDepKey)

    return MongoTxManagerAdapter(client=client)
