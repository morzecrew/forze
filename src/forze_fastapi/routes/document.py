"""Generated FastAPI routes for document aggregates.

Projects the document operations of a frozen registry (built with
:func:`forze_kits.aggregates.document.build_document_registry`, optionally merged
with :func:`forze_kits.aggregates.soft_deletion.build_soft_deletion_registry`)
onto a user-owned :class:`fastapi.APIRouter`. Request/response schemas come from
the operation descriptors, each route's ``operation_id`` is the registry operation
key verbatim, and every call runs through the normal operation pipeline
(``run_operation``), so plans, read-only enforcement, and hooks all apply.
Identity and invocation metadata are bound by the boundary middlewares, not here.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import AbstractSet, Mapping

from fastapi import APIRouter

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.document import DocumentKernelOp
from forze_kits.aggregates.soft_deletion import SoftDeletionKernelOp

from ._attach import (
    RouteBinding,
    RouteStyle,
    attach_operation_routes,
    body_endpoint,
    id_endpoint,
    id_rev_body_endpoint,
    id_rev_endpoint,
    resolve_namespace,
)

# ----------------------- #


def _rpc_path(op: object) -> str:
    """Operation-named path for an RPC binding (``/<op value>``)."""

    return f"/{op.value}"  # type: ignore[attr-defined]

# ----------------------- #

_REST_BINDINGS: Mapping[str, RouteBinding] = {
    DocumentKernelOp.GET: RouteBinding(method="GET", path="/{id}", build=id_endpoint),
    DocumentKernelOp.LIST: RouteBinding(
        method="POST", path="/list", build=body_endpoint
    ),
    DocumentKernelOp.RAW_LIST: RouteBinding(
        method="POST", path="/raw_list", build=body_endpoint
    ),
    DocumentKernelOp.LIST_CURSOR: RouteBinding(
        method="POST", path="/list_cursor", build=body_endpoint
    ),
    DocumentKernelOp.RAW_LIST_CURSOR: RouteBinding(
        method="POST", path="/raw_list_cursor", build=body_endpoint
    ),
    DocumentKernelOp.AGG_LIST: RouteBinding(
        method="POST", path="/agg_list", build=body_endpoint
    ),
    DocumentKernelOp.CREATE: RouteBinding(
        method="POST", path="", build=body_endpoint, status_code=201
    ),
    DocumentKernelOp.UPDATE: RouteBinding(
        method="PATCH", path="/{id}", build=id_rev_body_endpoint
    ),
    DocumentKernelOp.KILL: RouteBinding(
        method="DELETE", path="/{id}", build=id_endpoint, status_code=204
    ),
    # Soft-deletion ops are state transitions, not removals — they surface as
    # action sub-paths so the hard delete keeps ``DELETE /{id}``.
    SoftDeletionKernelOp.DELETE: RouteBinding(
        method="POST", path="/{id}/delete", build=id_rev_endpoint
    ),
    SoftDeletionKernelOp.RESTORE: RouteBinding(
        method="POST", path="/{id}/restore", build=id_rev_endpoint
    ),
}
"""Resource-style bindings per kernel operation."""

_RPC_LIST_OPS = (
    DocumentKernelOp.LIST,
    DocumentKernelOp.RAW_LIST,
    DocumentKernelOp.LIST_CURSOR,
    DocumentKernelOp.RAW_LIST_CURSOR,
    DocumentKernelOp.AGG_LIST,
)
"""RPC operations whose filter body has no query-param mapping — kept ``POST``."""

_RPC_BINDINGS: Mapping[str, RouteBinding] = {
    # Reads and id-addressed writes mirror the REST verbs but carry the id (and
    # rev) as query parameters on the operation-named path, so the surface is
    # linkable/cacheable instead of an opaque POST body.
    DocumentKernelOp.GET: RouteBinding(
        method="GET", path=_rpc_path(DocumentKernelOp.GET), build=id_endpoint
    ),
    DocumentKernelOp.CREATE: RouteBinding(
        method="POST",
        path=_rpc_path(DocumentKernelOp.CREATE),
        build=body_endpoint,
        status_code=201,
    ),
    DocumentKernelOp.UPDATE: RouteBinding(
        method="PATCH",
        path=_rpc_path(DocumentKernelOp.UPDATE),
        build=id_rev_body_endpoint,
    ),
    DocumentKernelOp.KILL: RouteBinding(
        method="DELETE",
        path=_rpc_path(DocumentKernelOp.KILL),
        build=id_endpoint,
        status_code=204,
    ),
    # Soft-delete and restore are reversible state transitions, not removals —
    # PATCH the resource (id + rev as query params, no body).
    SoftDeletionKernelOp.DELETE: RouteBinding(
        method="PATCH",
        path=_rpc_path(SoftDeletionKernelOp.DELETE),
        build=id_rev_endpoint,
    ),
    SoftDeletionKernelOp.RESTORE: RouteBinding(
        method="PATCH",
        path=_rpc_path(SoftDeletionKernelOp.RESTORE),
        build=id_rev_endpoint,
    ),
    # List/aggregate operations carry filter/sort/pagination bodies that do not
    # map onto query parameters — POST with the input DTO as body.
    **{
        op.value: RouteBinding(
            method="POST", path=_rpc_path(op), build=body_endpoint
        )
        for op in _RPC_LIST_OPS
    },
}
"""Operation-named bindings using REST verbs with id/rev as query parameters;
``create``/list operations keep ``POST`` with the input DTO as body."""


# ....................... #


def attach_document_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace | None = None,
    ctx_dep: ExecutionContextFactory,
    style: RouteStyle,
    include: AbstractSet[DocumentKernelOp | SoftDeletionKernelOp | str] | None = None,
    resource: str | None = None,
    path_overrides: (
        Mapping[DocumentKernelOp | SoftDeletionKernelOp | str, str] | None
    ) = None,
    exclude_none: bool = True,
) -> APIRouter:
    """Attach the registered document operations under *ns* to *router*.

    One route per registered kernel operation — :class:`DocumentKernelOp` plus,
    when a merged soft-deletion registry provides them, :class:`SoftDeletionKernelOp`
    (``delete``/``restore``). Operations the registry omits (e.g. writes on a
    read-only spec) are skipped, so capability-awareness mirrors the registry
    builders. Each route's ``operation_id`` is the operation key verbatim
    (e.g. ``notes.get``); schemas come from the operation descriptors. With
    ``style="rest"``, ``create`` targets the router's prefix root — give the
    router (or ``include_router``) a prefix.

    Both styles use REST verbs; they differ only in how a resource is addressed.
    REST puts the id in the path (``GET /{id}``); RPC keeps one operation-named
    path per operation and puts the id in a query parameter
    (``GET /notes.get?id=``), so the catalog still maps one-to-one.

    Args:
        router (APIRouter): A plain FastAPI router the caller owns.
        registry (FrozenOperationRegistry): Frozen registry holding the document
            operations.
        ns (StrKeyNamespace | None): Namespace the operations were registered under
            (e.g. ``spec.default_namespace``). Mutually exclusive with *resource* —
            provide exactly one.
        ctx_dep (ExecutionContextFactory): Factory yielding the current execution
            context per request.
        style (RouteStyle): ``"rest"`` for resource paths (``GET /{id}``,
            ``PATCH /{id}?rev=``, ``DELETE /{id}``, ``POST /{id}/delete|restore``;
            list operations stay ``POST /<op>`` since their filter bodies have no
            REST verb) or ``"rpc"`` for operation-named paths with the same verbs and
            the id/rev as query parameters (``create`` and list operations keep
            ``POST /<op>`` with the input DTO as body).
        include (AbstractSet | None): Optional narrowing to a subset of kernel
            operations; including an operation the registry lacks is a configuration
            error.
        resource (str | None): Convenience alternative to *ns* — a prefix string the
            namespace is built from (``StrKeyNamespace(prefix=resource)``); must equal
            the prefix the operations were registered under. Mutually exclusive with
            *ns* — provide exactly one.
        path_overrides (Mapping | None): Optional per-operation route-path replacements
            (keyed like *include*). Only the path changes; the ``operation_id`` stays
            verbatim. An override must bind exactly the default path's
            ``{id}``/``{rev}`` placeholders.

    Returns:
        APIRouter: The same *router*, for chaining.

    Raises:
        CoreException: On a configuration error — an unknown *include*/override
            operation, both or neither of *ns*/*resource*, or a path override that
            drops or adds a placeholder.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=resolve_namespace(ns, resource),
        ctx_dep=ctx_dep,
        bindings=_REST_BINDINGS if style == "rest" else _RPC_BINDINGS,
        include=include,
        path_overrides=path_overrides,
        exclude_none=exclude_none,
    )
