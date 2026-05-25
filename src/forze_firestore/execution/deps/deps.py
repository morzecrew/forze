"""Factory functions for Firestore document and tx manager adapters."""

from typing import Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.transaction import (
    AfterCommitPort,
    TransactionManagerPort,
)
from forze.application.coordinators import DocumentCacheCoordinator
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ...adapters import FirestoreDocumentAdapter, FirestoreTxManagerAdapter
from .._logger import logger
from .configs import FirestoreDocumentConfig, FirestoreReadOnlyDocumentConfig
from .keys import FirestoreClientDepKey
from .utils import doc_write_gw, read_gw

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableFirestoreReadOnlyDocument(DocumentQueryDepPort[R]):
    config: FirestoreReadOnlyDocumentConfig

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, Any, Any, Any],
    ) -> FirestoreDocumentAdapter[R, Any, Any, Any]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config["read"],
            tenant_aware=self.config.get("tenant_aware", False),
        )

        after_commit: AfterCommitPort | None = None

        if cache is not None:
            after_commit = ctx.tx.run_or_defer

        cc = DocumentCacheCoordinator[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return FirestoreDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            cache_coord=cc,
            batch_size=self.config.get("batch_size", 200),
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableFirestoreDocument(DocumentCommandDepPort[R, D, C, U]):
    config: FirestoreDocumentConfig

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, D, C, U],
    ) -> FirestoreDocumentAdapter[R, D, C, U]:
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

        history_relation = config.get("history")

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
            write_relation=config["write"],
            history_relation=history_relation,
            history_enabled=spec.history_enabled,
            tenant_aware=tenant_aware,
        )

        after_commit: AfterCommitPort | None = None

        if cache is not None:
            after_commit = ctx.tx.run_or_defer

        cc = DocumentCacheCoordinator[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return FirestoreDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            cache_coord=cc,
            batch_size=config.get("batch_size", 200),
        )


def firestore_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    client = context.deps.provide(FirestoreClientDepKey)
    return FirestoreTxManagerAdapter(client=client)
