"""Typing protocol for document adapter mixins."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol, Sequence
from uuid import UUID

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.document.gateways import (
    DocumentReadGatewayPort,
    DocumentWriteGatewayPort,
)
from forze.application.contracts.querying import QuerySortExpression

from .cache import DocumentCache
from ._types import C, D, R, U

if TYPE_CHECKING:
    from forze.application.contracts.base import CountlessPage
    from forze.application.contracts.querying import (
        PaginationExpression,
        QueryFilterExpression,
    )
    from forze.base.primitives import JsonDict

# ----------------------- #


class DocumentAdapterProtocol(Protocol, Generic[R, D, C, U]):
    """Structural type for the composed :class:`~.adapter.DocumentAdapter`."""

    spec: DocumentSpec[R, D, C, U]
    read_gw: DocumentReadGatewayPort[R]
    write_gw: DocumentWriteGatewayPort[D, C, U] | None
    document_cache: DocumentCache[R]
    batch_size: int
    enforce_primary_key_cursor_sort: bool
    hydrate_from_write: bool
    max_scan_pages: int | None
    max_stream_pages: int | None
    max_chunked_command_pages: int | None

    @property
    def _read_fields(self) -> frozenset[str]: ...

    @property
    def eff_batch_size(self) -> int: ...

    def _eff_stream_chunk_size(self, chunk_size: int) -> int: ...

    def _resolve_sorts(
        self,
        sorts: QuerySortExpression | None,
    ) -> QuerySortExpression: ...

    async def _to_read(self, domain: D | None, *, pk: UUID | None = None) -> R: ...

    async def _to_read_many(
        self,
        domains: Sequence[D | None],
        *,
        pks: Sequence[UUID] | None = None,
    ) -> Sequence[R]: ...

    def _require_write(self) -> DocumentWriteGatewayPort[D, C, U]: ...

    async def _finalize_single_write(
        self,
        domain: D,
        *,
        return_new: bool,
        pk: UUID | None = None,
    ) -> R | None: ...

    async def _finalize_bulk_write(
        self,
        domains: Sequence[D],
        *,
        return_new: bool,
        pks: Sequence[UUID] | None = None,
    ) -> Sequence[R] | None: ...

    async def project_many(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]: ...


# ....................... #


class DocumentQueryDelegateMixin(Generic[R]):
    """Query port methods supplied by :class:`~._query.DocumentQueryMixin` in the MRO.

    Empty at runtime; documents the command→query dependency for type checkers when
    :class:`~._command.DocumentCommandMixin` is composed ahead of the command mixin on
    :class:`~.adapter.DocumentAdapter`.
    """

    if TYPE_CHECKING:

        async def project_many(
            self,
            fields: Sequence[str],
            filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
            pagination: PaginationExpression | None = None,
            sorts: QuerySortExpression | None = None,
        ) -> CountlessPage[JsonDict]: ...


# ....................... #


class DocumentAdapterMixinBase(Generic[R, D, C, U]):
    """Typing-only base declaring attrs available on composed coordinator mixins."""

    if TYPE_CHECKING:
        spec: DocumentSpec[R, D, C, U]
        read_gw: DocumentReadGatewayPort[R]
        write_gw: DocumentWriteGatewayPort[D, C, U] | None
        document_cache: DocumentCache[R]
        batch_size: int
        enforce_primary_key_cursor_sort: bool
        hydrate_from_write: bool
        max_scan_pages: int | None
        max_stream_pages: int | None
        max_chunked_command_pages: int | None

        @property
        def _read_fields(self) -> frozenset[str]: ...

        @property
        def eff_batch_size(self) -> int: ...

        def _eff_stream_chunk_size(self, chunk_size: int) -> int: ...

        def _resolve_sorts(
            self,
            sorts: QuerySortExpression | None,
        ) -> QuerySortExpression: ...

        async def _to_read(self, domain: D | None, *, pk: UUID | None = None) -> R: ...

        async def _to_read_many(
            self,
            domains: Sequence[D | None],
            *,
            pks: Sequence[UUID] | None = None,
        ) -> Sequence[R]: ...

        def _require_write(self) -> DocumentWriteGatewayPort[D, C, U]: ...

        async def _finalize_single_write(
            self,
            domain: D,
            *,
            return_new: bool,
            pk: UUID | None = None,
        ) -> R | None: ...

        async def _finalize_bulk_write(
            self,
            domains: Sequence[D],
            *,
            return_new: bool,
            pks: Sequence[UUID] | None = None,
        ) -> Sequence[R] | None: ...
