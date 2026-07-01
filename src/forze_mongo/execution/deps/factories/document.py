"""Mongo document dep factories."""

from typing import Any, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    BytesCipherPort,
    DeterministicCipherDepKey,
    EncryptionTier,
    KeyringDepKey,
)
from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.transaction import AfterCommitPort
from forze.application.execution import ExecutionContext
from forze.application.execution.domain import domain_dispatcher_provider
from forze.application.integrations.crypto import resolve_document_codecs
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


def _cache_cipher(
    ctx: ExecutionContext, spec: DocumentSpec[Any, Any, Any, Any]
) -> BytesCipherPort | None:
    """Keyring for sealing cache bodies — only when the document field-encrypts and a
    keyring is wired (so the cache does not re-expose what the document protects)."""

    if spec.encryption is None or spec.encryption.is_empty:
        return None

    return ctx.deps.provide(KeyringDepKey) if ctx.deps.exists(KeyringDepKey) else None


def _resolve_codecs(
    ctx: ExecutionContext,
    spec: DocumentSpec[Any, Any, Any, Any],
    *,
    required_encryption: EncryptionTier | None = None,
) -> DocumentCodecs[Any, Any, Any, Any]:
    """Spec codecs, wrapped for field encryption when ``spec.encryption`` is set.

    Resolves the ciphers as optional (``None`` when unregistered) so the shared
    helper can fail closed with a precise error instead of the generic dependency
    lookup raising.
    """

    return resolve_document_codecs(
        spec.resolved_codecs,
        spec_name=str(spec.name),
        encryption=spec.encryption,
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
        integration="mongo",
        code="mongo.document.encryption_wiring",
        required_encryption=required_encryption,
    )

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoReadOnlyDocument(DocumentQueryDepPort[R]):
    """Configurable Mongo read-only document adapter."""

    config: MongoReadOnlyDocumentConfig = attrs.field(
        validator=attrs.validators.instance_of(MongoReadOnlyDocumentConfig),
    )
    """Configuration for the document."""

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum field-encryption coverage for this deployment (``None`` = no floor)."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DocumentSpec[R, Any, Any, Any],
    ) -> MongoDocumentAdapter[R, Any, Any, Any]:
        cache = ctx.cache(spec.cache) if spec.cache is not None else None

        codecs = _resolve_codecs(
            ctx, spec, required_encryption=self.required_encryption
        )

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=self.config.read,
            tenant_aware=self.config.tenant_aware,
            codec=codecs.read,
            read_validation=self.config.read_validation,
            computed_null_ordering=self.config.computed_null_ordering,
            lenient_read_fields=spec.resolved_lenient_read_fields,
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
            cipher=_cache_cipher(ctx, spec),
            cipher_tenant=ctx.inv_ctx.get_tenant,
        )

        if cache is not None:
            # Cancel this cache's detached early-refresh tasks at shutdown before the
            # backing clients close (a cacheless coordinator spawns none, so skip it).
            ctx.background_owners.register(cc)

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

    required_encryption: EncryptionTier | None = attrs.field(default=None)
    """Declared minimum field-encryption coverage for this deployment (``None`` = no floor)."""

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

        codecs = _resolve_codecs(
            ctx, spec, required_encryption=self.required_encryption
        )

        read = read_gw(
            ctx,
            read_type=spec.read,
            read_relation=config.read,
            tenant_aware=tenant_aware,
            codec=codecs.read,
            read_validation=config.read_validation,
            computed_null_ordering=config.computed_null_ordering,
            lenient_read_fields=spec.resolved_lenient_read_fields,
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
            write_omit_fields=spec.write_omit_fields,
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
            cipher=_cache_cipher(ctx, spec),
            cipher_tenant=ctx.inv_ctx.get_tenant,
        )

        if cache is not None:
            # Cancel this cache's detached early-refresh tasks at shutdown before the
            # backing clients close (a cacheless coordinator spawns none, so skip it).
            ctx.background_owners.register(cc)

        return MongoDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            document_cache=cc,
            batch_size=config.batch_size,
            dispatcher_provider=domain_dispatcher_provider(ctx),
        )
