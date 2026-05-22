from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
from forze.application.execution.registry import OperationRegistry
from forze.application.handlers.search import (
    CursorSearch,
    ProjectedCursorSearch,
    ProjectedSearch,
    Search,
)
from forze.base.primitives import StrKeyNamespace

from .operations import SearchKernelOp
from .value_objects import SearchMappers

# ----------------------- #


def build_search_registry[M: BaseModel](
    spec: SearchSpec[M],
    mappers: SearchMappers = SearchMappers(),
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build search operation registry.

    :param spec: Search specification.
    :param dtos: Search DTO specification.
    :param mappers: Search mappers.
    :param ns: Optional namespace.
    :returns: Operation registry with all supported operations.
    """

    ns = ns or spec.default_namespace

    reg = OperationRegistry(
        handlers={
            ns.key(SearchKernelOp.TYPED): lambda ctx: Search(
                search=ctx.search.query(spec),
                mapper=mappers.search(ctx) if mappers.search else None,
            ),
            ns.key(SearchKernelOp.RAW): lambda ctx: ProjectedSearch(
                search=ctx.search.query(spec),
                mapper=(
                    mappers.projected_search(ctx) if mappers.projected_search else None
                ),
            ),
            ns.key(SearchKernelOp.TYPED_CURSOR): lambda ctx: CursorSearch(
                search=ctx.search.query(spec),
                mapper=mappers.cursor_search(ctx) if mappers.cursor_search else None,
            ),
            ns.key(SearchKernelOp.RAW_CURSOR): lambda ctx: ProjectedCursorSearch(
                search=ctx.search.query(spec),
                mapper=(
                    mappers.projected_search_cursor(ctx)
                    if mappers.projected_search_cursor
                    else None
                ),
            ),
        },
    )

    return reg


# ....................... #


def build_hub_search_registry[M: BaseModel](
    spec: HubSearchSpec[M],
    mappers: SearchMappers = SearchMappers(),
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build hub search operation registry.

    :param spec: Hub search specification.
    :param mappers: Search mappers.
    :param ns: Optional namespace.
    :returns: Operation registry with all supported operations.
    """

    ns = ns or spec.default_namespace

    reg = OperationRegistry(
        handlers={
            ns.key(SearchKernelOp.TYPED): lambda ctx: Search(
                search=ctx.search.hub(spec),
                mapper=mappers.search(ctx) if mappers.search else None,
            ),
            ns.key(SearchKernelOp.RAW): lambda ctx: ProjectedSearch(
                search=ctx.search.hub(spec),
                mapper=(
                    mappers.projected_search(ctx) if mappers.projected_search else None
                ),
            ),
            ns.key(SearchKernelOp.TYPED_CURSOR): lambda ctx: CursorSearch(
                search=ctx.search.hub(spec),
                mapper=mappers.cursor_search(ctx) if mappers.cursor_search else None,
            ),
            ns.key(SearchKernelOp.RAW_CURSOR): lambda ctx: ProjectedCursorSearch(
                search=ctx.search.hub(spec),
                mapper=(
                    mappers.projected_search_cursor(ctx)
                    if mappers.projected_search_cursor
                    else None
                ),
            ),
        },
    )
    return reg


# ....................... #


def build_federated_search_registry[M: BaseModel](
    spec: FederatedSearchSpec[M],
    mappers: SearchMappers = SearchMappers(),
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build federated search operation registry.

    :param spec: Federated search specification.
    :param mappers: Search mappers.
    :param ns: Optional namespace.
    :returns: Operation registry with all supported operations.
    """

    ns = ns or spec.default_namespace

    reg = OperationRegistry(
        handlers={
            ns.key(SearchKernelOp.TYPED): lambda ctx: Search(
                search=ctx.search.federated(spec),
                mapper=mappers.search(ctx) if mappers.search else None,
            ),
            ns.key(SearchKernelOp.TYPED_CURSOR): lambda ctx: CursorSearch(
                search=ctx.search.federated(spec),
                mapper=mappers.cursor_search(ctx) if mappers.cursor_search else None,
            ),
        },
    )
    return reg
