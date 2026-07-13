from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.execution.operations.facade import (
    OperationFacade,
    OperationFacadeFactory,
    facade_op,
    namespaced_facade,
)
from forze.domain.models import BaseDTO

from .handlers import (
    AggregatedListDocuments,
    CreateDocument,
    CursorListDocuments,
    GetDocument,
    KillDocument,
    ListDocuments,
    ProjectedCursorListDocuments,
    ProjectedListDocuments,
    UpdateDocument,
)
from .operations import DocumentKernelOp

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.execution import ExecutionRuntime
    from forze.application.execution.operations import FrozenOperationRegistry
    from forze.base.primitives import StrKeyNamespace

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

# ....................... #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentFacade(OperationFacade, Generic[R, C, U]):
    """Typed facade for document operations."""

    get = facade_op(
        DocumentKernelOp.GET,
        uc=GetDocument[R],
    )
    """Get document operation."""

    list = facade_op(
        DocumentKernelOp.LIST,
        uc=ListDocuments[R],
    )
    """List documents operation."""

    raw_list = facade_op(
        DocumentKernelOp.RAW_LIST,
        uc=ProjectedListDocuments,
    )
    """Raw list documents operation."""

    list_cursor = facade_op(
        DocumentKernelOp.LIST_CURSOR,
        uc=CursorListDocuments[R],
    )
    """List documents with cursor (keyset) pagination operation."""

    raw_list_cursor = facade_op(
        DocumentKernelOp.RAW_LIST_CURSOR,
        uc=ProjectedCursorListDocuments,
    )
    """Raw list with cursor (keyset) pagination operation."""

    agg_list = facade_op(
        DocumentKernelOp.AGG_LIST,
        uc=AggregatedListDocuments,
    )
    """List documents with aggregates operation."""

    create = facade_op(
        DocumentKernelOp.CREATE,
        uc=CreateDocument[C, Any, R],
    )
    """Create document operation."""

    update = facade_op(
        DocumentKernelOp.UPDATE,
        uc=UpdateDocument[U, Any, R],
    )
    """Update document operation."""

    kill = facade_op(
        DocumentKernelOp.KILL,
        uc=KillDocument,
    )
    """Kill document operation."""


# ....................... #


def document_facade(
    runtime: ExecutionRuntime,
    registry: FrozenOperationRegistry,
    spec: DocumentSpec[R, Any, C, U],
    *,
    namespace: StrKeyNamespace | None = None,
) -> OperationFacadeFactory[DocumentFacade[R, C, U]]:
    """Build a per-call :class:`DocumentFacade` factory bound to *runtime*'s context.

    Returns a zero-argument callable that yields a fresh, fully-typed facade on each call,
    reading the runtime's *current* scope context — so it is safe to build once at startup
    and call per request; it never caches a context across scopes. The namespace defaults to
    the spec's, matching :func:`build_document_registry`.

    Replaces the hand-written factory — ``def users(): return DocumentFacade(
    ctx=runtime.get_context(), registry=registry, namespace=...)`` — with one call::

        users = document_facade(runtime, registry, user_spec)
        await users().create(cmd)
    """

    factory: OperationFacadeFactory[DocumentFacade[R, C, U]] = OperationFacadeFactory(
        type=DocumentFacade,
        registry=registry,
        ctx_factory=runtime.get_context,
        ns=namespace if namespace is not None else spec.default_namespace,
    )

    return factory
