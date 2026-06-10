"""Generated FastAPI routes for search aggregates.

Projects the search operations of a frozen registry (built with
:func:`forze_kits.aggregates.search.build_search_registry` or its hub/federated
siblings) onto a user-owned :class:`fastapi.APIRouter`. Search requests are
filter/query bodies with no natural REST verb, so there is no style choice —
every operation is ``POST /<op>`` with the input DTO as body, mirroring the
catalog one-to-one. Schemas come from the operation descriptors (federated
results are heterogeneous, so those routes declare no response model) and each
route's ``operation_id`` is the registry operation key verbatim.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import AbstractSet, Mapping

from fastapi import APIRouter

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.search import SearchKernelOp

from ._attach import RouteBinding, attach_operation_routes, body_endpoint

# ----------------------- #

_SEARCH_BINDINGS: Mapping[str, RouteBinding] = {
    op.value: RouteBinding(method="POST", path=f"/{op.value}", build=body_endpoint)
    for op in SearchKernelOp
}
"""Uniform ``POST /<op>`` bindings per search kernel operation."""


# ....................... #


def attach_search_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    include: AbstractSet[SearchKernelOp | str] | None = None,
) -> APIRouter:
    """Attach the registered search operations under *ns* to *router*.

    One ``POST /<op>`` route per registered :class:`SearchKernelOp`. Operations
    the registry omits (e.g. raw projections on a federated registry) are
    skipped, so capability-awareness mirrors the registry builders. Each route's
    ``operation_id`` is the operation key verbatim (e.g. ``notes.typed``).

    :param router: A plain FastAPI router the caller owns.
    :param registry: Frozen registry holding the search operations.
    :param ns: Namespace the operations were registered under
        (e.g. ``spec.default_namespace``).
    :param ctx_dep: Factory yielding the current execution context per request.
    :param include: Optional narrowing to a subset of kernel operations; including
        an operation the registry lacks is a configuration error.
    :returns: *router*, for chaining.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=ns,
        ctx_dep=ctx_dep,
        bindings=_SEARCH_BINDINGS,
        include=include,
    )
