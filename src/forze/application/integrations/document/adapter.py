"""Document adapter orchestrating query/command ports over gateways."""

from collections.abc import Callable
from functools import cached_property
from typing import Sequence
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
    validate_query_parameters,
)
from forze.application.contracts.document.gateways import (
    DocumentReadGatewayPort,
    DocumentWriteGatewayPort,
)
from forze.application.contracts.domain import DomainEventDispatcherPort
from forze.application.contracts.querying import (
    QuerySortExpression,
    read_fields_for_model,
    resolve_effective_sorts,
)
from forze.base.exceptions import exc


from ..._logger import logger
from .cache import DocumentCache
from ._command import DocumentCommandMixin
from ._limits import (
    DEFAULT_MAX_CHUNKED_COMMAND_PAGES,
    DEFAULT_MAX_SCAN_PAGES,
    DEFAULT_MAX_STREAM_PAGES,
)
from ._query import DocumentQueryMixin
from ._types import C, D, R, U

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentAdapter(
    DocumentQueryMixin[R],
    DocumentCommandMixin[R, D, C, U],
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
):
    """Orchestrate document query/command ports over pluggable persistence gateways.

    MRO places :class:`~._query.DocumentQueryMixin` before
    :class:`~._command.DocumentCommandMixin` so command helpers (e.g.
    :meth:`~._command.DocumentCommandMixin.update_matching_strict`) can call
    :meth:`~._query.DocumentQueryMixin.project_many`.
    """

    spec: DocumentSpec[R, D, C, U]
    read_gw: DocumentReadGatewayPort[R]
    write_gw: DocumentWriteGatewayPort[D, C, U] | None = attrs.field(default=None)
    document_cache: DocumentCache[R]
    batch_size: int = 200
    enforce_primary_key_cursor_sort: bool = False

    max_scan_pages: int | None = DEFAULT_MAX_SCAN_PAGES  # type: ignore[override]
    """Max offset-scan pages when ``limit`` is omitted; ``None`` for unlimited."""

    max_stream_pages: int | None = DEFAULT_MAX_STREAM_PAGES  # type: ignore[override]
    """Max cursor pages per stream; ``None`` for unlimited."""

    max_chunked_command_pages: int | None = DEFAULT_MAX_CHUNKED_COMMAND_PAGES  # type: ignore[override]
    """Max pages for :meth:`update_matching_strict`; ``None`` for unlimited."""

    dispatcher_provider: Callable[[], DomainEventDispatcherPort | None] = attrs.field(
        default=lambda: None
    )
    """Resolve the in-process domain-event dispatcher (or ``None`` when unregistered).

    Injected by the integration factory from ``ctx.domain()``; defaults to a no-op so a
    non-aggregate document never requires a dispatcher.
    """

    # ....................... #

    def with_parameters(self, params: BaseModel) -> "DocumentAdapter[R, D, C, U]":
        """Bind typed query parameters (default: validate the contract, then fail closed).

        Validates *params* against the spec's ``query_params`` contract, then refuses — a backend
        that can apply query parameters (as query-scoped session settings the relation reads)
        overrides this to return a param-bound clone. The default keeps unsupporting backends
        honest: they reject the call instead of silently ignoring the parameters.
        """

        validate_query_parameters(self.spec, params)
        raise exc.precondition(
            f"Document route {str(self.spec.name)!r}: this backend does not support query "
            "parameters.",
            code="query_parameters_unsupported",
        )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        """Check compatibility of document cache with read gateway and specification."""

        if self.document_cache.read_model_type is not self.read_gw.model_type:
            raise exc.configuration(
                "Document document cache read model type mismatches read gateway model type."
            )

        if self.document_cache.document_name != self.spec.name:
            raise exc.configuration(
                "Document document cache name mismatches document specification name."
            )

    # ....................... #

    @property
    def tenant_aware(self) -> bool:
        """Whether the backing storage partitions rows by tenant.

        Simply returns ``self.read_gw.tenant_aware``; this property performs no
        validation itself. When a ``write_gw`` is present, read/write tenant-awareness
        consistency is enforced separately by
        :func:`~forze.application.integrations.document.hydration.validate_read_write_gateway_compat`,
        so the read gateway's value reflects the adapter as a whole.
        """

        return self.read_gw.tenant_aware

    # ....................... #

    @cached_property
    def hydrate_from_write(self) -> bool:  # type: ignore[override]
        """Whether reads hydrate missing domain state from the write model.

        Computed once on first access via :meth:`_compute_hydrate_from_write`;
        subclasses override that hook (default ``False``)."""

        return self._compute_hydrate_from_write()

    # ....................... #

    def _compute_hydrate_from_write(self) -> bool:
        """Hook for backends that can hydrate reads from the write model."""

        return False

    # ....................... #

    @cached_property
    def _read_fields(self) -> frozenset[str]:  # type: ignore[override]
        return read_fields_for_model(self.spec.read)

    # ....................... #

    def _resolve_sorts(
        self,
        sorts: QuerySortExpression | None,
    ) -> QuerySortExpression:
        return resolve_effective_sorts(
            sorts=sorts,
            default_sort=self.spec.default_sort,
            read_fields=self._read_fields,
            spec_name=self.spec.name,
            model=self.spec.read,
        )

    # ....................... #

    @cached_property
    def eff_batch_size(self) -> int:  # type: ignore[override]
        if self.batch_size < 10:
            logger.warning("Batch size is too small, using default value of 200")

            return 200

        if self.batch_size > 20000:
            logger.warning("Batch size is too large, using default value of 200")

            return 200

        return self.batch_size

    # ....................... #

    def _eff_stream_chunk_size(self, chunk_size: int) -> int:
        if chunk_size < 10:
            logger.warning("Stream chunk size is too small, using default value of 500")
            return 500

        if chunk_size > 20000:
            logger.warning("Stream chunk size is too large, using default value of 500")
            return 500

        return chunk_size

    # ....................... #

    async def _to_read(self, domain: D | None, *, pk: UUID | None = None) -> R:
        if self.hydrate_from_write and domain is not None:
            return self.read_gw.read_codec.transform(domain)

        doc_pk = domain.id if domain is not None else pk

        if doc_pk is None:
            raise exc.internal(
                "Cannot load read model after write: domain row missing and no primary key.",
                code="document_hydration_failed",
            )

        return await self.read_gw.get(doc_pk)

    # ....................... #

    async def _to_read_many(
        self,
        domains: Sequence[D | None],
        *,
        pks: Sequence[UUID] | None = None,
    ) -> Sequence[R]:
        if not domains:
            if pks:
                return await self.read_gw.get_many(list(pks))

            return []

        if self.hydrate_from_write and all(d is not None for d in domains):
            return self.read_gw.read_codec.transform_many(domains)  # type: ignore[arg-type]

        if pks is not None and len(pks) != len(domains):
            raise exc.internal(
                "Primary keys length must match domain rows for read-back.",
                code="document_hydration_failed",
            )

        keys: list[UUID] = []

        for i, domain in enumerate(domains):
            if domain is not None:
                keys.append(domain.id)
            elif pks is not None:
                keys.append(pks[i])
            else:
                raise exc.internal(
                    "Cannot load read models after write: domain row missing and no primary key.",
                    code="document_hydration_failed",
                )

        return await self.read_gw.get_many(keys)

    # ....................... #

    def _require_write(self) -> DocumentWriteGatewayPort[D, C, U]:
        if self.write_gw is None:
            raise exc.configuration("Write gateway is not configured")

        return self.write_gw

    # ....................... #

    async def _finalize_single_write(
        self,
        domain: D,
        *,
        return_new: bool,
        pk: UUID | None = None,
    ) -> R | None:
        await self._dispatch_domain_events([domain])

        if not return_new:
            return None

        res = await self._to_read(domain, pk=pk)

        await self.document_cache.after_commit_or_now(
            lambda: self.document_cache.set_one(res)
        )

        return res

    # ....................... #

    async def _finalize_bulk_write(
        self,
        domains: Sequence[D],
        *,
        return_new: bool,
        pks: Sequence[UUID] | None = None,
    ) -> Sequence[R] | None:
        await self._dispatch_domain_events(domains)

        if not return_new:
            return None

        res = await self._to_read_many(domains, pks=pks)
        await self.document_cache.after_commit_or_now(
            lambda: self.document_cache.set_many(res)
        )

        return res
