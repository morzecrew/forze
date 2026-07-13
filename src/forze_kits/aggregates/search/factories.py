from typing import Any

from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    MultiSourceSearchOptions,
    SearchOptions,
    SearchSpec,
)
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKey, StrKeyNamespace

from .dto import (
    CursorSearchRequestDTO,
    ProjectedCursorSearchRequestDTO,
    ProjectedSearchCursorPaginated,
    ProjectedSearchPaginated,
    ProjectedSearchRequestDTO,
    SearchCursorPaginated,
    SearchPaginated,
    SearchRequestDTO,
)
from .handlers import (
    CursorSearch,
    ProjectedCursorSearch,
    ProjectedSearch,
    Search,
)
from .operations import SearchKernelOp
from .value_objects import SearchMappers

# ----------------------- #


def _parametrized(generic: Any, arg: Any) -> Any:
    """Parametrize a generic envelope with a runtime read type (off the static path)."""

    return generic[arg]


# ....................... #


def _request_dto(dto: Any, options_type: Any) -> Any:
    """The request DTO with its ``options`` shape pinned, or the bare default-typed class.

    ``options_type is None`` keeps the plain single-index :class:`SearchOptions` DTO (and its
    schema name); a multi-source type parametrizes the generic so the body accepts the extra
    member-selection / merge keys.
    """

    return dto if options_type is None else _parametrized(dto, options_type)


# ....................... #


def _typed_search_descriptors(
    model_type: type,
    *,
    sensitive: bool = False,
    options_type: Any = None,
) -> dict[StrKey, OperationDescriptor]:
    """Descriptors for the four single-index/hub search operations.

    A ``sensitive`` spec propagates the flag onto every descriptor so projection
    surfaces (generated routes, MCP) can refuse it at build time. ``options_type`` pins the
    request ``options`` shape — left ``None`` for single-index (:class:`SearchOptions`), set to
    :class:`MultiSourceSearchOptions` for hub requests.
    """

    return {
        SearchKernelOp.TYPED: OperationDescriptor(
            input_type=_request_dto(SearchRequestDTO, options_type),
            output_type=_parametrized(SearchPaginated, model_type),
            description="Full-text search with typed results (offset pagination).",
            sensitive=sensitive,
        ),
        SearchKernelOp.RAW: OperationDescriptor(
            input_type=_request_dto(ProjectedSearchRequestDTO, options_type),
            output_type=ProjectedSearchPaginated,
            description="Full-text search with field-projected results (offset pagination).",
            sensitive=sensitive,
        ),
        SearchKernelOp.TYPED_CURSOR: OperationDescriptor(
            input_type=_request_dto(CursorSearchRequestDTO, options_type),
            output_type=_parametrized(SearchCursorPaginated, model_type),
            description="Full-text search with typed results (cursor pagination).",
            sensitive=sensitive,
        ),
        SearchKernelOp.RAW_CURSOR: OperationDescriptor(
            input_type=_request_dto(ProjectedCursorSearchRequestDTO, options_type),
            output_type=ProjectedSearchCursorPaginated,
            description="Full-text search with field-projected results (cursor pagination).",
            sensitive=sensitive,
        ),
    }


# ....................... #

_ALL_SEARCH_OPS: tuple[SearchKernelOp, ...] = (
    SearchKernelOp.TYPED,
    SearchKernelOp.RAW,
    SearchKernelOp.TYPED_CURSOR,
    SearchKernelOp.RAW_CURSOR,
)
"""Single-index / hub search operations — all read-only (query)."""

# ....................... #


def build_search_registry[M: BaseModel](
    spec: SearchSpec[M],
    mappers: SearchMappers[SearchOptions] = SearchMappers(),
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
                mapper=(mappers.projected_search(ctx) if mappers.projected_search else None),
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

    reg = reg.bind(*_ALL_SEARCH_OPS, namespace=ns).as_query().finish()

    return reg.set_descriptors(
        _typed_search_descriptors(spec.model_type, sensitive=spec.sensitive),
        namespace=ns,
    )


# ....................... #


def build_hub_search_registry[M: BaseModel](
    spec: HubSearchSpec[M],
    mappers: SearchMappers[MultiSourceSearchOptions] = SearchMappers(),
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
                mapper=(mappers.projected_search(ctx) if mappers.projected_search else None),
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

    reg = reg.bind(*_ALL_SEARCH_OPS, namespace=ns).as_query().finish()

    # A hub projects every member's rows, so it is sensitive if any member is.
    return reg.set_descriptors(
        _typed_search_descriptors(
            spec.model_type,
            sensitive=any(member.sensitive for member in spec.members),
            options_type=MultiSourceSearchOptions,
        ),
        namespace=ns,
    )


# ....................... #


def build_federated_search_registry[M: BaseModel](
    spec: FederatedSearchSpec[M],
    mappers: SearchMappers[MultiSourceSearchOptions] = SearchMappers(),
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

    reg = (
        reg.bind(SearchKernelOp.TYPED, SearchKernelOp.TYPED_CURSOR, namespace=ns)
        .as_query()
        .finish()
    )

    # A federated surface projects every member's rows, so it is sensitive if any
    # member (or any nested hub member) is.
    sensitive = any(
        (
            member.sensitive
            if isinstance(member, SearchSpec)
            else any(leg.sensitive for leg in member.members)
        )
        for member in spec.members
    )

    # Federated search is heterogeneous (no single model type), so results carry no
    # single response schema — descriptors record the request shape only.
    return reg.set_descriptors(
        {
            SearchKernelOp.TYPED: OperationDescriptor(
                input_type=_request_dto(SearchRequestDTO, MultiSourceSearchOptions),
                description="Federated full-text search across members (offset pagination).",
                sensitive=sensitive,
            ),
            SearchKernelOp.TYPED_CURSOR: OperationDescriptor(
                input_type=_request_dto(CursorSearchRequestDTO, MultiSourceSearchOptions),
                description="Federated full-text search across members (cursor pagination).",
                sensitive=sensitive,
            ),
        },
        namespace=ns,
    )
