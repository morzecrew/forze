"""Firestore document dep factories."""

from typing import Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.transaction import AfterCommitPort
from forze.application.integrations.document import DocumentCache
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ....adapters import FirestoreDocumentAdapter
from ..._logger import logger
from ..configs import FirestoreDocumentConfig, FirestoreReadOnlyDocumentConfig
from ..utils import doc_write_gw, read_gw

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableFirestoreReadOnlyDocument(DocumentQueryDepPort[R]):
    """Configurable Firestore read-only document adapter."""

    config: FirestoreReadOnlyDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(FirestoreReadOnlyDocumentConfig),
    )
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, Any, Any, Any],
    ) -> FirestoreDocumentAdapter[R, Any, Any, Any]:
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

        cc = DocumentCache[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return FirestoreDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            document_cache=cc,
            batch_size=self.config.batch_size,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableFirestoreDocument(DocumentCommandDepPort[R, D, C, U]):
    """Configurable Firestore read-write document adapter."""

    config: FirestoreDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(FirestoreDocumentConfig),
    )
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, D, C, U],
    ) -> FirestoreDocumentAdapter[R, D, C, U]:
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

        history_relation = config.history

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
            write_relation=config.write,
            history_relation=history_relation,
            history_enabled=spec.history_enabled,
            tenant_aware=tenant_aware,
        )

        after_commit: AfterCommitPort | None = None

        if cache is not None:
            after_commit = ctx.tx_ctx.run_or_defer

        cc = DocumentCache[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return FirestoreDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            document_cache=cc,
            batch_size=config.batch_size,
        )
