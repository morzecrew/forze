"""Mongo document dep factories."""

from typing import Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.transaction import AfterCommitPort
from forze.application.execution import ExecutionContext
from forze.application.execution.domain import domain_dispatcher_provider
from forze.application.integrations.document import DocumentCache
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, Document

from ....adapters import MongoDocumentAdapter
from ..._logger import logger
from ..configs import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from ..utils import doc_write_gw, read_gw

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoReadOnlyDocument(DocumentQueryDepPort[R]):
    """Configurable Mongo read-only document adapter."""

    config: MongoReadOnlyDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(MongoReadOnlyDocumentConfig),
    )
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, Any, Any, Any],
    ) -> MongoDocumentAdapter[R, Any, Any, Any]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None

        codecs = spec.resolved_codecs

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config.read,
            tenant_aware=self.config.tenant_aware,
            codec=codecs.read,
            read_validation=self.config.read_validation,
            computed_null_ordering=self.config.computed_null_ordering,
        )

        after_commit: AfterCommitPort | None = None

        if cache is not None:
            after_commit = ctx.tx_ctx.run_or_defer

        cc = DocumentCache[R](
            read_model_type=read.model_type,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
            cache_spec=spec.cache,
            tenant_key=lambda: (
                str(t.tenant_id) if (t := ctx.inv_ctx.get_tenant()) else None
            ),
            read_codec=read.read_codec,
        )

        return MongoDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            document_cache=cc,
            batch_size=self.config.batch_size,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoDocument(DocumentCommandDepPort[R, D, C, U]):
    """Configurable Mongo document adapter."""

    config: MongoDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(MongoDocumentConfig),
    )
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

        codecs = spec.resolved_codecs

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=config.read,
            tenant_aware=tenant_aware,
            codec=codecs.read,
            read_validation=config.read_validation,
            computed_null_ordering=config.computed_null_ordering,
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
            codecs=codecs,
            write_relation=write_relation,
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
            cache_spec=spec.cache,
            tenant_key=lambda: (
                str(t.tenant_id) if (t := ctx.inv_ctx.get_tenant()) else None
            ),
            read_codec=read.read_codec,
        )

        return MongoDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            document_cache=cc,
            batch_size=config.batch_size,
            dispatcher_provider=domain_dispatcher_provider(ctx),
        )
