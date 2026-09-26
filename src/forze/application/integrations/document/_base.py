"""Shared bases for the document adapter mixins."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Generic
from uuid import UUID

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.document.gateways import (
    DocumentReadGatewayPort,
    DocumentWriteGatewayPort,
)
from forze.base.exceptions import CoreException, ExceptionKind

from ._types import C, D, R, U
from .cache import DocumentCache

if TYPE_CHECKING:
    from forze.application.contracts.base import CountlessPage
    from forze.application.contracts.domain import DomainEventDispatcherPort
    from forze.application.contracts.querying import (
        PaginationExpression,
        QueryFilterExpression,
        QuerySortExpression,
    )
    from forze.base.primitives import JsonDict


_TAGS_NOT_FOUND = "__forze_tags_not_found__"


def _tagging_not_found[**P, T](
    method: Callable[P, Awaitable[T]],
) -> Callable[P, Awaitable[T]]:
    @functools.wraps(method)
    async def tagged(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await method(*args, **kwargs)

        except CoreException as error:
            if error.kind is ExceptionKind.NOT_FOUND and error.resource_type is None:
                error.resource_type = str(args[0].spec.name)  # type: ignore[attr-defined]

            raise

    setattr(tagged, _TAGS_NOT_FOUND, True)
    return tagged


class DocumentNotFoundTagging:
    """Tag a not-found escaping any public coroutine method with the adapter's spec name.

    A missing row is then an error *about a resource type*, which a non-disclosing
    :class:`~forze.base.exceptions.DenialPosture` renders exactly like a denial of the same
    type. Applied per class, including every subclass's own overrides, so a backend that
    overrides a method cannot drop the tag. A not-found that already names a type keeps it.
    """

    if TYPE_CHECKING:
        spec: DocumentSpec[Any, Any, Any, Any]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        for name in dir(cls):
            if name.startswith("_"):
                continue

            method = getattr(cls, name, None)

            if inspect.iscoroutinefunction(method) and not getattr(method, _TAGS_NOT_FOUND, False):
                setattr(cls, name, _tagging_not_found(method))


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
        dispatcher_provider: Callable[[], DomainEventDispatcherPort | None]

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
