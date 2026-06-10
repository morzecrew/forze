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
)

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

_RPC_BINDINGS: Mapping[str, RouteBinding] = {
    **{
        op.value: RouteBinding(method="POST", path=f"/{op.value}", build=body_endpoint)
        for op in (*DocumentKernelOp, *SoftDeletionKernelOp)
    },
    DocumentKernelOp.KILL: RouteBinding(
        method="POST",
        path=f"/{DocumentKernelOp.KILL.value}",
        build=body_endpoint,
        status_code=204,
    ),
}
"""Uniform ``POST /<op>`` bindings; ``kill`` has no output and answers 204."""


# ....................... #


def attach_document_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    style: RouteStyle,
    include: AbstractSet[DocumentKernelOp | SoftDeletionKernelOp | str] | None = None,
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

    :param router: A plain FastAPI router the caller owns.
    :param registry: Frozen registry holding the document operations.
    :param ns: Namespace the operations were registered under
        (e.g. ``spec.default_namespace``).
    :param ctx_dep: Factory yielding the current execution context per request.
    :param style: ``"rest"`` for resource paths (``GET /{id}``, ``PATCH /{id}?rev=``,
        ``DELETE /{id}``, ``POST /{id}/delete|restore``; list operations stay
        ``POST /<op>`` since their filter bodies have no REST verb) or ``"rpc"``
        for uniform ``POST /<op>`` with the input DTO as body.
    :param include: Optional narrowing to a subset of kernel operations; including
        an operation the registry lacks is a configuration error.
    :returns: *router*, for chaining.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=ns,
        ctx_dep=ctx_dep,
        bindings=_REST_BINDINGS if style == "rest" else _RPC_BINDINGS,
        include=include,
    )
