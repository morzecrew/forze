"""Postgres document dep factories."""

from typing import TYPE_CHECKING, Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    DeterministicCipherDepKey,
    EncryptionTier,
    KeyringDepKey,
)
from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentCommandDepPort,
    DocumentQueryDepPort,
)
from forze.application.execution.domain import domain_dispatcher_provider
from forze.application.integrations.crypto import resolve_document_codecs
from forze.application.integrations.document import DocumentCache
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, Document

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
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def _resolve_codecs(
    ctx: "ExecutionContext",
    spec: "DocumentSpec[Any, Any, Any, Any]",
    *,
    required_encryption: EncryptionTier | None = None,
) -> DocumentCodecs[Any, Any, Any, Any]:
    """Spec codecs, wrapped for field encryption when ``encrypted_fields`` is set.

    Resolves the ciphers as optional (``None`` when unregistered) so the shared
    helper can fail closed with a precise error instead of the generic dependency
    lookup raising.
    """

    return resolve_document_codecs(
        spec.resolved_codecs,
        spec_name=str(spec.name),
        encrypted_fields=spec.encrypted_fields,
        searchable_fields=spec.searchable_fields,
        keyring=(
            ctx.deps.provide(KeyringDepKey)
            if ctx.deps.exists(KeyringDepKey)
            else None
        ),
        deterministic=(
            ctx.deps.provide(DeterministicCipherDepKey)
            if ctx.deps.exists(DeterministicCipherDepKey)
            else None
        ),
        tenant_provider=ctx.inv_ctx.get_tenant,
        integration="postgres",
        code="postgres.document.encryption_wiring",
        required_encryption=required_encryption,
        bind_record_id=spec.encryption_binds_record_id,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePostgresReadOnlyDocument(DocumentQueryDepPort[R]):
    """Configurable Postgres read-only document adapter."""

    config: PostgresReadOnlyDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(PostgresReadOnlyDocumentConfig),
    )
    """Configuration for the document."""

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum field-encryption coverage for this deployment (``None`` = no floor)."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: "DocumentSpec[R, Any, Any, Any]",
    ) -> PostgresDocumentAdapter[R, Any, Any, Any]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None

        codecs = _resolve_codecs(
            ctx, spec, required_encryption=self.required_encryption
        )

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config.read,
            tenant_aware=self.config.tenant_aware,
            nested_field_hints=self.config.nested_field_hints,
            codec=codecs.read,
            read_validation=self.config.read_validation,
        )

        after_commit: "AfterCommitPort | None" = None

        if cache is not None:
            after_commit = ctx.tx_ctx.run_or_defer

        cc = DocumentCache[R](
            read_model_type=read.model_type,
            read_codec=read.read_codec,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
            cache_spec=spec.cache,
            tenant_key=lambda: (
                str(t.tenant_id) if (t := ctx.inv_ctx.get_tenant()) else None
            ),
        )

        return PostgresDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            document_cache=cc,
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

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum field-encryption coverage for this deployment (``None`` = no floor)."""

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

        codecs = _resolve_codecs(
            ctx, spec, required_encryption=self.required_encryption
        )

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config.read,
            tenant_aware=tenant_aware,
            nested_field_hints=self.config.nested_field_hints,
            codec=codecs.read,
            read_validation=self.config.read_validation,
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
            codecs=codecs,
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

        cc = DocumentCache[R](
            read_model_type=read.model_type,
            read_codec=read.read_codec,
            document_name=spec.name,
            cache=cache,
            after_commit=after_commit,
            cache_spec=spec.cache,
            tenant_key=lambda: (
                str(t.tenant_id) if (t := ctx.inv_ctx.get_tenant()) else None
            ),
        )

        return PostgresDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            document_cache=cc,
            batch_size=self.config.batch_size,
            dispatcher_provider=domain_dispatcher_provider(ctx),
        )
