"""Project an :class:`~forze_kits.aggregates.AggregateKit`'s slice onto a FastAPI router.

The kit's composed registry already holds the document, soft-delete, and (when declared) external
search operations under their own namespaces. This is the one-call routes emitter over it: it
projects the document + soft-delete operations (via :func:`attach_document_routes`) and, when the kit
declares a ``search``, the search query operations (via :func:`attach_search_routes`) — from the same
frozen registry, so every route's ``operation_id`` is the registry operation key verbatim and
capability-awareness mirrors the kit (an op the kit omits gets no route).

It lives in ``forze_fastapi`` (not on the kit) so the kit stays transport-agnostic — the routes are
one projection of the slice, the deps module another.
"""

from __future__ import annotations

from forze_fastapi._compat import require_fastapi

require_fastapi()

from typing import Any

from fastapi import APIRouter

from forze.application.execution.context import ExecutionContextFactory
from forze.base.primitives import StrKey
from forze_kits.aggregates import AggregateKit

from ._attach import RouteStyle
from .document import attach_document_routes
from .search import attach_search_routes

# ----------------------- #


def attach_aggregate_routes(
    router: APIRouter,
    kit: AggregateKit[Any, Any, Any, Any],
    *,
    ctx_dep: ExecutionContextFactory,
    style: RouteStyle = "rest",
    tx_route: StrKey = "default",
    exclude_none: bool = True,
) -> APIRouter:
    """Attach *kit*'s document, soft-delete, and search routes to *router*.

    The routes **execute** through the composed registry (``run_operation``), so *tx_route* is
    load-bearing — pass the same route the deps module registers its transaction manager under (and
    the same one the kit's :meth:`~forze_kits.aggregates.AggregateKit.facade` uses). The document +
    soft-delete routes take *style* (``"rest"``/``"rpc"``); the search routes are ``POST``-only.

    Args:
        router (APIRouter): A plain FastAPI router the caller owns.
        kit (AggregateKit): The declared aggregate whose slice is projected.
        ctx_dep (ExecutionContextFactory): Factory yielding the current execution context per request.
        style (RouteStyle): Resource-path (``"rest"``) or operation-named (``"rpc"``) document routes.
        tx_route (StrKey): Transaction route the write ops run on — must match the deps module.
        exclude_none (bool): Drop ``None`` fields from response bodies (default ``True``).

    Returns:
        APIRouter: The same *router*, for chaining.
    """

    registry = kit.registry(tx_route=tx_route)

    attach_document_routes(
        router,
        registry=registry,
        ns=kit.spec.default_namespace,
        ctx_dep=ctx_dep,
        style=style,
        exclude_none=exclude_none,
    )

    if kit.search is not None:
        attach_search_routes(
            router,
            registry=registry,
            ns=kit.search.default_namespace,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    return router
