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
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_kits.aggregates import AggregateKit

from ._attach import RouteStyle
from .document import attach_document_routes
from .search import attach_search_routes
from .storage import attach_storage_routes

# ----------------------- #


def attach_aggregate_routes(
    router: APIRouter,
    kit: AggregateKit[Any, Any, Any, Any],
    *,
    ctx_dep: ExecutionContextFactory,
    style: RouteStyle = "rest",
    tx_route: StrKey = "default",
    storage_prefix: str = "/blobs",
    exclude_none: bool = True,
) -> APIRouter:
    """Attach *kit*'s document, soft-delete, and search routes to *router*.

    The routes **execute** through the composed registry (``run_operation``), so *tx_route* is
    load-bearing — pass the same route the deps module registers its transaction manager under (and
    the same one the kit's :meth:`~forze_kits.aggregates.AggregateKit.facade` uses). The document +
    soft-delete (and, when declared, object-storage) routes take *style* (``"rest"``/``"rpc"``); the
    search routes are ``POST``-only.

    Args:
        router (APIRouter): A plain FastAPI router the caller owns.
        kit (AggregateKit): The declared aggregate whose slice is projected.
        ctx_dep (ExecutionContextFactory): Factory yielding the current execution context per request.
        style (RouteStyle): Resource-path (``"rest"``) or operation-named (``"rpc"``) document routes.
        tx_route (StrKey): Transaction route the write ops run on — must match the deps module.
        storage_prefix (str): Sub-path the blob routes mount under (default ``"/blobs"``) — object
            storage is a separate resource, and REST ``upload`` would otherwise collide with the
            document ``create`` at ``POST /``. Only used when the kit declares ``storage``.
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

    if kit.storage is not None:
        # Blobs are a separate resource under their own sub-path — REST ``upload`` (POST /) would
        # otherwise collide with the document ``create`` on the router root, so a root-like prefix
        # is rejected rather than silently letting one operation shadow the other.
        if not storage_prefix.startswith("/") or storage_prefix == "/":
            raise exc.configuration(
                f"storage_prefix must be a non-root sub-path like '/blobs' (got "
                f"{storage_prefix!r}) — otherwise the blob routes collide with the document "
                f"routes on the router root",
            )

        blob_router = APIRouter(prefix=storage_prefix)
        attach_storage_routes(
            blob_router,
            registry=registry,
            ns=kit.storage.default_namespace,
            ctx_dep=ctx_dep,
            style=style,
            exclude_none=exclude_none,
        )
        router.include_router(blob_router)

    return router
