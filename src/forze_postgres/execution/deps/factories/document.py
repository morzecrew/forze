"""Postgres document dep factories."""

from typing import TYPE_CHECKING, Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
)
from forze.application.coordinators import DocumentCacheCoordinator
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ....adapters import PostgresDocumentAdapter
from ..._logger import logger
from ..configs import PostgresDocumentConfig, PostgresReadOnlyDocumentConfig
from ..utils import doc_write_gw, read_gw

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.contracts.transaction import AfterCommitPort
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresReadOnlyDocument(DocumentQueryDepPort[R]):
    """Configurable Postgres read-only document adapter."""

    config: PostgresReadOnlyDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(PostgresReadOnlyDocumentConfig),
    )
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: "DocumentSpec[R, Any, Any, Any]",
    ) -> PostgresDocumentAdapter[R, Any, Any, Any]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config.read,
            tenant_aware=self.config.tenant_aware,
            nested_field_hints=self.config.nested_field_hints,
        )

        after_commit: "AfterCommitPort | None" = None

        if cache is not None:
            after_commit = ctx.tx_ctx.run_or_defer

        cc = DocumentCacheCoordinator[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return PostgresDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            cache_coord=cc,
            batch_size=self.config.batch_size,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresDocument(DocumentCommandDepPort[R, D, C, U]):
    """Configurable Postgres document adapter."""

    config: PostgresDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(PostgresDocumentConfig),
    )
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: "DocumentSpec[R, D, C, U]",
    ) -> PostgresDocumentAdapter[R, D, C, U]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None
        tenant_aware = self.config.tenant_aware

        if spec.write is None:
            raise exc.internal(
                "Write relation is required for non read-only documents."
            )

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config.read,
            tenant_aware=tenant_aware,
            nested_field_hints=self.config.nested_field_hints,
        )

        history_relation = self.config.history
        bookkeeping_strategy = self.config.bookkeeping_strategy

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
            write_relation=self.config.write,
            history_relation=history_relation,
            history_enabled=spec.history_enabled,
            bookkeeping_strategy=bookkeeping_strategy,
            tenant_aware=tenant_aware,
            nested_field_hints=self.config.nested_field_hints,
            conflict_target=self.config.conflict_target,
        )

        after_commit: "AfterCommitPort | None" = None

        if cache is not None:
            after_commit = ctx.tx_ctx.run_or_defer

        cc = DocumentCacheCoordinator[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
        )

        return PostgresDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            cache_coord=cc,
            batch_size=self.config.batch_size,
        )
